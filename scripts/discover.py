#!/usr/bin/env python3
"""Discover new Spark and Delta releases on Maven Central and update the module POMs and matrix.json.

Rules (docs/superpowers/specs/2026-10-27-version-automation-design.md, Decided 2-6), applied in this order:
  2. a new Spark patch in a module's line moves the module and its rows on the old patch; scala.version and
     hadoop.aws.version follow the spark-parent POM when it changed between the two patches, and only upward;
  3. a new Delta patch moves every row and the module on the older patch (pinned rows keep their Spark patch);
  4. a new Delta minor adds rows for the module's current Spark patch and shrinks the previous current-Delta rows;
  5. a Spark line that has no module yet becomes a module once delta-spark_<minor>_2.13 exists.
A Delta version counts for a module only if its per-Spark artifact delta-spark_<minor>_<scala> carries it; the
unsuffixed delta-spark_<scala> is used only for a Spark minor that has no per-Spark artifact (Spark 3.5), within the
module's Delta major. Discovery never changes the Spark version of a row pinned to another Spark patch.
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


def uses_per(m, dv):
    return bool(stable(dv.get(per_spark(m), [])))


def pool(m, dv, artifact=None):
    """Stable Delta versions that count for the module: its per-Spark artifact, else (Spark minors without one)
    the given or default artifact."""
    if uses_per(m, dv):
        return stable(dv[per_spark(m)])
    return stable(dv.get(artifact or m["delta_artifact"], []))


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


def adopt_per_default(m, rows, dv):
    """After the module's Delta moved: the module default becomes the per-Spark artifact that carries it, so the
    published POM resolves what the current rows test. Rows naming it drop the key; rows that inherited the old
    default and whose Delta the new one lacks name the old artifact."""
    per = per_spark(m)
    if m["delta_artifact"] == per or m["delta"] not in stable(dv.get(per, [])):
        return
    old, m["delta_artifact"] = m["delta_artifact"], per
    have = set(stable(dv[per]))
    for i, r in enumerate(rows):
        if r["module"] == name(m):
            if r.get("deltaArtifact") == per:
                rows[i] = {k: v for k, v in r.items() if k != "deltaArtifact"}
            elif "deltaArtifact" not in r and r["delta"] not in have:
                rows[i] = with_artifact(r, old, m)


def new_line_candidates(modules, spark_versions, dv):
    """Scala 2.13 Spark lines with no module yet, above the oldest 4.x module that has a per-Spark Delta artifact."""
    mods = [m for m in modules if m["scala"] == "2.13"]
    floor = [line(m["spark"]) for m in mods if vkey(m["spark"])[0] >= 4 and uses_per(m, dv)]
    floor = min(floor) if floor else max(line(m["spark"]) for m in mods)
    have = {line(m["spark"]) for m in mods}
    return sorted({line(v) for v in stable(spark_versions.get("2.13", [])) if line(v) > floor} - have)


def plan_updates(modules, rows, spark_versions, delta_versions, spark_parent):
    """Apply rules 2-6. spark_versions: {scala binary: [versions]}; delta_versions: {artifact: [versions]};
    spark_parent(scala binary, spark version) -> {"hadoop", "scala"}. Returns (modules, rows, additions)."""
    modules, rows = [dict(m) for m in modules], [dict(r) for r in rows]
    dv = delta_versions
    adds = []

    def report(kind, found, notes=None):
        for v in sorted(found, key=vkey):
            extra = "".join(f"; {n}" for n in (notes or {}).get(v, []))
            adds.append(f"{kind} {v} ({', '.join(found[v])}{extra})")

    found, notes = {}, {}  # rule 2: Spark patches
    for m in modules:
        new = newest_patch(spark_versions.get(m["scala"], []), m["spark"])
        if not new:
            continue
        old_parent, new_parent = spark_parent(m["scala"], m["spark"]), spark_parent(m["scala"], new)
        for r in rows:
            if r["module"] == name(m) and r["spark"] == m["spark"]:
                r["spark"] = new
        # Spark's parent POM can carry an older Scala or Hadoop than the module pins: follow only a change, upward
        for key, label in (("scala_version", "scala"), ("hadoop", "hadoop-aws")):
            src = key.split("_")[0]
            if new_parent[src] != old_parent[src] and vkey(new_parent[src]) > vkey(m[key]):
                m[key] = new_parent[src]
                note = f"{label} {m[key]}"
                if note not in notes.setdefault(new, []):
                    notes[new].append(note)
        m["spark"] = new
        found.setdefault(new, []).append(name(m))
    report("spark", found, notes)

    found = {}  # rule 3: Delta patches; every row on the old patch follows, pinned Spark patches stay as they are
    for m in modules:
        per = uses_per(m, dv)
        for i, r in enumerate(rows):
            if r["module"] != name(m):
                continue
            explicit = r.get("deltaArtifact")
            new = newest_patch(pool(m, dv, explicit or m["delta_artifact"]), r["delta"])
            if new:
                if per:
                    art = explicit if explicit and new in stable(dv.get(explicit, [])) else per_spark(m)
                else:
                    art = explicit or m["delta_artifact"]
                rows[i] = with_artifact(dict(r, delta=new), art, m)
                found.setdefault(new, [])
                if name(m) not in found[new]:
                    found[new].append(name(m))
        new = newest_patch(pool(m, dv), m["delta"])
        if new:
            m["delta"] = new
            adopt_per_default(m, rows, dv)
            found.setdefault(new, [])
            if name(m) not in found[new]:
                found[new].append(name(m))
    report("delta", found)

    found = {}  # rule 4: Delta minors (current rows only)
    for m in modules:
        versions = pool(m, dv)
        if not uses_per(m, dv):
            versions = [v for v in versions if vkey(v)[0] == vkey(m["delta"])[0]]
        for ln in sorted({line(v) for v in versions if line(v) > line(m["delta"])}):
            new = [v for v in versions if line(v) == ln][-1]
            cur = [r for r in rows if r["module"] == name(m) and r["spark"] == m["spark"] and r["delta"] == m["delta"]]
            if not cur:
                continue
            art = per_spark(m) if uses_per(m, dv) else m["delta_artifact"]
            copies = [with_artifact(dict(r, delta=new), art, m) for r in cur]
            keep = min(cur, key=lambda r: vkey(r["jdk"]))
            rows = [r for r in rows if r is keep or not any(r is c for c in cur)]
            keep["smoke"] = False
            last = max(i for i, r in enumerate(rows) if r["module"] == name(m))
            rows[last + 1:last + 1] = copies
            m["delta"] = new
            adopt_per_default(m, rows, dv)
            found.setdefault(new, []).append(name(m))
    report("delta", found)

    spark213 = stable(spark_versions.get("2.13", []))  # rule 5: new Spark lines, in version order
    for ln in new_line_candidates(modules, spark_versions, dv):
        minor = f"{ln[0]}.{ln[1]}"
        artifact = f"delta-spark_{minor}_2.13"
        delta = stable(dv.get(artifact, []))
        if not delta:
            continue
        spark = [v for v in spark213 if line(v) == ln][-1]
        pred = [m for m in modules if m["scala"] == "2.13" and line(m["spark"]) < ln][-1]
        parent = spark_parent("2.13", spark)
        new = dict(pred, minor=minor, spark=spark, delta=delta[-1], delta_artifact=artifact,
                   scala_version=higher(pred["scala_version"], parent["scala"]),
                   hadoop=higher(pred["hadoop"], parent["hadoop"]))
        copies = []
        for r in rows:
            if r["module"] == name(pred) and r["spark"] == pred["spark"] and r["delta"] == pred["delta"]:
                c = dict(r, module=name(new), spark=spark, delta=delta[-1])
                c.pop("deltaArtifact", None)
                copies.append(c)
        at = max((i + 1 for i, r in enumerate(rows) if r["module"] == name(pred)), default=len(rows))
        rows[at:at] = copies
        modules.insert(modules.index(pred) + 1, new)
        adds.append(f"module {name(new)} (spark {spark}, delta {delta[-1]})")
    return modules, rows, adds


def new_lines(modules, spark_versions, dv):
    """Per-Spark Delta artifacts to fetch for the candidate new Spark lines."""
    return [f"delta-spark_{a}.{b}_2.13" for a, b in new_line_candidates(modules, spark_versions, dv)]


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


def add_module(parent_pom_text, module_dir, after=None):
    """Insert the <module> line after the entry for `after` (default: the last one)."""
    lines = list(re.finditer(r"^( *)<module>([^<]*)</module>\n", parent_pom_text, flags=re.M))
    at = next((x for x in lines if x.group(2) == after), lines[-1])
    return (parent_pom_text[:at.end()] + f"{at.group(1)}<module>{module_dir}</module>\n"
            + parent_pom_text[at.end():])


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
            parent = f[f"spark-parent:{scala}:{version}"]
        except KeyError:
            sys.exit(f"discover: no fake spark-parent:{scala}:{version}")
    else:
        a = f"spark-parent_{scala}"
        parent = parse_spark_parent(get(f"{CENTRAL}/org/apache/spark/{a}/{version}/{a}-{version}.pom"))
    if not (STABLE.match(parent["hadoop"]) and STABLE.match(parent["scala"])
            and parent["scala"].startswith(scala + ".")):
        sys.exit(f"discover: spark-parent_{scala} {version}: unexpected values {parent}")
    return parent


# ---- driver -----------------------------------------------------------------------------------------------------

POM_PROPS = {"spark": "spark.version", "delta": "delta.version", "scala_version": "scala.version",
             "hadoop": "hadoop.aws.version"}


def apply(root, old_modules, modules, rows, old_rows):
    """Compute every file in memory first, then write the ones that differ."""
    old = {name(m): m for m in old_modules}
    parent = (root / "pom.xml").read_text()
    changelog = (root / "CHANGELOG.md").read_text()
    poms = {}
    for i, m in enumerate(modules):
        n = name(m)
        if n in old:
            text = (root / "modules" / n / "pom.xml").read_text()
            for key, prop in POM_PROPS.items():
                if m[key] != old[n][key]:
                    text = edit_pom(text, prop, m[key])
            if m["delta_artifact"] != old[n]["delta_artifact"]:
                text = set_delta_artifact(text, m["delta_artifact"])
        else:
            prev = modules[i - 1]
            text = new_module_pom(poms[name(prev)], m["minor"], m["spark"], m["delta"], m["scala_version"],
                                  m["hadoop"])
            parent = add_module(parent, f"modules/{n}", after=f"modules/{name(prev)}")
            changelog = changelog_unreleased(
                changelog, f"Add `gluecat-{n}` for Spark {m['spark']} with Delta {m['delta']}.")
        poms[n] = text
    files = {root / "modules" / n / "pom.xml": t for n, t in poms.items()}
    files[root / "pom.xml"] = parent
    files[root / "CHANGELOG.md"] = changelog
    if rows != old_rows:
        files[root / "matrix.json"] = write_matrix(rows)
    for path, text in files.items():
        if not path.exists() or path.read_text() != text:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)


def main(argv=None, root=ROOT):
    ap = argparse.ArgumentParser(description=__doc__, epilog=EPILOG,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="print the additions, write nothing")
    args = ap.parse_args(argv)
    modules, rows = render_tables.read_modules(root), render_tables.read_rows(root)
    spark = {s: fetch_versions("org.apache.spark", f"spark-sql_{s}") for s in dict.fromkeys(m["scala"] for m in modules)}
    artifacts = ({m["delta_artifact"] for m in modules} | {per_spark(m) for m in modules}
                 | {r["deltaArtifact"] for r in rows if "deltaArtifact" in r})
    delta = {a: fetch_versions("io.delta", a) for a in sorted(artifacts)}
    for a in new_lines(modules, spark, delta):
        delta[a] = fetch_versions("io.delta", a)
    new_modules, new_rows, adds = plan_updates(modules, rows, spark, delta, fetch_spark_parent)
    if adds:
        print("\n".join(adds))
    if adds and not args.dry_run:
        apply(root, modules, new_modules, new_rows, rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
