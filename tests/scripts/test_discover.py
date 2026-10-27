import contextlib
import io
import json
import os
import shutil
import sys
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = Path(__file__).resolve().parent / "fixtures" / "repo"  # frozen copy of the tree; never follows the live files
sys.path.insert(0, str(ROOT / "scripts"))
import discover  # noqa: E402
import render_tables  # noqa: E402


def mod(minor, spark, delta, scala="2.13", art=None, java="17", sv="2.13.18", hadoop="3.4.1"):
    return {"minor": minor, "scala": scala, "spark": spark, "delta": delta, "java": java, "scala_version": sv,
            "hadoop": hadoop, "delta_artifact": art or f"delta-spark_{minor}_{scala}"}


def row(module, spark, delta, jdk, smoke=False, **extra):
    r = {"module": module, "spark": spark, "delta": delta}
    r.update(extra)
    r.update({"jdk": jdk, "smoke": smoke})
    return r


def parent(scala="2.13.19", hadoop="3.5.0", old=("4.2.0",), old_values=("2.13.18", "3.4.1")):
    """spark-parent stub: the old Spark patches carry old_values, every other version the given ones."""
    return lambda s, v: ({"scala": old_values[0], "hadoop": old_values[1]} if v in old
                         else {"scala": scala, "hadoop": hadoop})


def plan(modules, rows, spark=None, delta=None, par=None):
    return discover.plan_updates(modules, rows, spark or {}, delta or {}, par or parent())


M42 = "spark-4.2_2.13"


def m42():
    return mod("4.2", "4.2.0", "4.4.1", art="delta-spark_2.13")


def rows42():
    return [row(M42, "4.2.0", "4.4.1", "17", True), row(M42, "4.2.0", "4.4.1", "21"),
            row(M42, "4.2.0", "4.4.1", "25", True)]


class SparkPatchTest(unittest.TestCase):  # rule 2
    def test_current_rows_and_module_move_pinned_stay(self):
        rows = [row(M42, "4.2.0", "4.4.1", "17", True), row(M42, "4.1.1", "4.1.0", "17")]
        mods, out, adds = plan([m42()], rows, spark={"2.13": ["4.2.0", "4.2.1", "4.3.0"]}, par=parent("2.13.18", "3.4.1"))
        self.assertEqual(mods[0]["spark"], "4.2.1")
        self.assertEqual([r["spark"] for r in out], ["4.2.1", "4.1.1"])
        self.assertEqual(adds, ["spark 4.2.1 (spark-4.2_2.13)"])

    def test_scala_and_hadoop_follow_a_spark_parent_change(self):
        mods, _, adds = plan([m42()], rows42(), spark={"2.13": ["4.2.1"]}, par=parent("2.13.19", "3.5.0"))
        self.assertEqual((mods[0]["scala_version"], mods[0]["hadoop"]), ("2.13.19", "3.5.0"))
        self.assertEqual(adds, ["spark 4.2.1 (spark-4.2_2.13: scala 2.13.19, hadoop-aws 3.5.0)"])

    def test_only_the_changed_value_is_reported(self):
        mods, _, adds = plan([m42()], rows42(), spark={"2.13": ["4.2.1"]}, par=parent("2.13.18", "3.5.1"))
        self.assertEqual((mods[0]["scala_version"], mods[0]["hadoop"]), ("2.13.18", "3.5.1"))
        self.assertEqual(adds, ["spark 4.2.1 (spark-4.2_2.13: hadoop-aws 3.5.1)"])

    def test_unchanged_spark_parent_leaves_the_module_alone(self):
        # the module pins 3.4.1 while Spark's parent says 3.5.0 for both patches: no change, no note
        mods, _, adds = plan([m42()], rows42(), spark={"2.13": ["4.2.1"]}, par=parent("2.13.18", "3.5.0",
                             old=("4.2.0", "4.2.1"), old_values=("2.13.18", "3.5.0")))
        self.assertEqual((mods[0]["hadoop"], adds), ("3.4.1", ["spark 4.2.1 (spark-4.2_2.13)"]))

    def test_parent_values_never_lower_the_module(self):
        mods, _, _ = plan([m42()], rows42(), spark={"2.13": ["4.2.1"]}, par=parent("2.13.8", "3.3.4"))
        self.assertEqual((mods[0]["scala_version"], mods[0]["hadoop"]), ("2.13.18", "3.4.1"))

    def test_numeric_comparison_and_stable_only(self):
        m = mod("4.2", "4.2.9", "4.4.1")
        mods, _, _ = plan([m], [], spark={"2.13": ["4.2.10", "4.2.11-rc1", "4.2.9", "4.2.12-preview1"]})
        self.assertEqual(mods[0]["spark"], "4.2.10")

    def test_other_minor_is_not_a_patch(self):
        mods, _, adds = plan([m42()], rows42(), spark={"2.13": ["4.2.0", "4.3.0"]})
        self.assertEqual((mods[0]["spark"], adds), ("4.2.0", []))

    def test_scala_binary_versions_are_separate(self):
        a = mod("3.5", "3.5.3", "3.3.3", scala="2.12", art="delta-spark_2.12", sv="2.12.21")
        b = mod("3.5", "3.5.3", "3.3.3", scala="2.13", art="delta-spark_2.13")
        mods, _, adds = plan([a, b], [], spark={"2.12": ["3.5.3"], "2.13": ["3.5.3", "3.5.4"]})
        self.assertEqual([m["spark"] for m in mods], ["3.5.3", "3.5.4"])
        self.assertEqual(adds, ["spark 3.5.4 (spark-3.5_2.13)"])


class DeltaPatchTest(unittest.TestCase):  # rule 3
    def test_rows_and_module_move_to_new_patch(self):
        d = {"delta-spark_2.13": ["4.4.1", "4.4.2"]}
        mods, out, adds = plan([m42()], rows42(), delta=d)
        self.assertEqual(mods[0]["delta"], "4.4.2")
        self.assertEqual({r["delta"] for r in out}, {"4.4.2"})
        self.assertEqual(adds, ["delta 4.4.2 (spark-4.2_2.13)"])

    def test_pinned_rows_follow_the_delta_patch_but_keep_their_spark(self):
        rows = rows42() + [row(M42, "4.1.1", "4.4.1", "17"), row(M42, "4.1.1", "4.3.0", "17")]
        _, out, _ = plan([m42()], rows, delta={"delta-spark_2.13": ["4.3.0", "4.4.1", "4.4.2"]})
        self.assertEqual([(r["spark"], r["delta"]) for r in out[-2:]], [("4.1.1", "4.4.2"), ("4.1.1", "4.3.0")])

    def test_floor_rows_follow(self):
        m = mod("3.5", "3.5.9", "3.3.3", art="delta-spark_2.13", java="8")
        n = "spark-3.5_2.13"
        rows = [row(n, "3.5.3", "3.3.3", "8", True), row(n, "3.5.9", "3.3.3", "8")]
        mods, out, _ = plan([m], rows, delta={"delta-spark_2.13": ["3.3.3", "3.3.4"]})
        self.assertEqual([(r["spark"], r["delta"]) for r in out], [("3.5.3", "3.3.4"), ("3.5.9", "3.3.4")])
        self.assertEqual(mods[0]["delta"], "3.3.4")

    def test_row_artifact_decides_availability(self):
        r = [row(M42, "4.2.0", "4.1.0", "17", deltaArtifact="delta-spark_2.13")]
        _, out, adds = plan([m42()], r, delta={"delta-spark_2.13": ["4.1.0"], "elsewhere": ["4.1.1"]})
        self.assertEqual((out[0]["delta"], adds), ("4.1.0", []))
        _, out, _ = plan([m42()], r, delta={"delta-spark_2.13": ["4.1.0", "4.1.1"]})
        self.assertEqual(out[0], row(M42, "4.2.0", "4.1.1", "17"))  # the module default is that artifact

    def test_moved_pinned_row_gets_the_per_spark_artifact_after_delta(self):
        rows = rows42() + [row(M42, "4.1.1", "4.3.0", "17")]
        d = {"delta-spark_4.2_2.13": ["4.3.0", "4.3.1", "4.4.1"], "delta-spark_2.13": ["4.3.0", "4.4.1"]}
        mods, out, _ = plan([m42()], rows, delta=d)
        self.assertEqual(list(out[-1]), ["module", "spark", "delta", "deltaArtifact", "jdk", "smoke"])
        self.assertEqual((out[-1]["delta"], out[-1]["deltaArtifact"]), ("4.3.1", "delta-spark_4.2_2.13"))
        self.assertEqual(mods[0]["delta_artifact"], "delta-spark_2.13")  # the module itself did not move

    def test_no_unsuffixed_fallback_when_the_per_spark_artifact_exists(self):
        d = {"delta-spark_4.2_2.13": ["4.4.1"], "delta-spark_2.13": ["4.4.1", "4.4.2"]}
        mods, out, adds = plan([m42()], rows42(), delta=d)
        self.assertEqual((mods[0]["delta"], out, adds), ("4.4.1", rows42(), []))

    def test_moved_row_with_an_explicit_artifact_keeps_it_when_that_carries_the_version(self):
        a = mod("4.1", "4.1.3", "4.4.1")
        n = "spark-4.1_2.13"
        rows = [row(n, "4.1.1", "4.1.0", "17", deltaArtifact="delta-spark_2.13"),
                row(n, "4.1.1", "4.2.0", "17", deltaArtifact="delta-spark_2.13")]
        d = {"delta-spark_4.1_2.13": ["4.1.0", "4.1.1", "4.2.0", "4.2.1", "4.4.1"],
             "delta-spark_2.13": ["4.1.0", "4.1.1", "4.2.0"]}
        _, out, _ = plan([a], rows, delta=d)
        self.assertEqual(out[0], row(n, "4.1.1", "4.1.1", "17", deltaArtifact="delta-spark_2.13"))
        self.assertEqual(out[1], row(n, "4.1.1", "4.2.1", "17"))  # the per-Spark artifact is this module's default

    def test_no_key_when_the_module_default_is_the_per_spark_artifact(self):
        a = mod("4.1", "4.1.3", "4.4.1")
        rows = [row("spark-4.1_2.13", "4.1.3", "4.4.1", "17", True, deltaArtifact="delta-spark_2.13")]
        _, out, _ = plan([a], rows, delta={"delta-spark_4.1_2.13": ["4.4.1", "4.4.2"]})
        self.assertEqual(out[0], row("spark-4.1_2.13", "4.1.3", "4.4.2", "17", True))

    def test_no_artifact_no_line(self):
        _, out, adds = plan([m42()], rows42(), delta={})
        self.assertEqual((out, adds), (rows42(), []))

    def test_one_line_lists_every_module(self):
        a, b = mod("4.1", "4.1.3", "4.4.1"), m42()
        d = {"delta-spark_4.1_2.13": ["4.4.2"], "delta-spark_2.13": ["4.4.2"]}
        _, _, adds = plan([a, b], [], delta=d)
        self.assertEqual(adds, ["delta 4.4.2 (spark-4.1_2.13; spark-4.2_2.13)"])


class DeltaMinorTest(unittest.TestCase):  # rule 4
    D = {"delta-spark_2.13": ["4.4.1", "4.5.0"]}

    def test_rows_copied_old_shrunk_module_bumped(self):
        mods, out, adds = plan([m42()], rows42(), delta=self.D)
        self.assertEqual(mods[0]["delta"], "4.5.0")
        self.assertEqual(adds, ["delta 4.5.0 (spark-4.2_2.13)"])
        self.assertEqual([(r["delta"], r["jdk"], r["smoke"]) for r in out],
                         [("4.4.1", "17", False), ("4.5.0", "17", True), ("4.5.0", "21", False),
                          ("4.5.0", "25", True)])

    def test_new_rows_follow_the_module_not_the_end_of_the_file(self):
        other = row("spark-4.3_2.13", "4.3.0", "4.5.0", "17", True)
        _, out, _ = plan([m42()], rows42() + [other], delta=self.D)
        self.assertIs(out[-1]["module"] == "spark-4.3_2.13", True)
        self.assertEqual([r["delta"] for r in out[:-1]], ["4.4.1", "4.5.0", "4.5.0", "4.5.0"])

    def test_only_modules_with_the_artifact(self):
        a = mod("4.1", "4.1.3", "4.4.1")
        ra = [row("spark-4.1_2.13", "4.1.3", "4.4.1", "17", True)]
        mods, out, adds = plan([a, m42()], ra + rows42(), delta=self.D)
        self.assertEqual((mods[0]["delta"], mods[1]["delta"]), ("4.4.1", "4.5.0"))
        self.assertEqual(adds, ["delta 4.5.0 (spark-4.2_2.13)"])
        self.assertEqual(out[0], ra[0])

    def test_pinned_rows_not_copied(self):
        rows = [row(M42, "4.1.1", "4.4.1", "17")] + rows42()
        _, out, _ = plan([m42()], rows, delta=self.D)
        self.assertEqual(sum(r["delta"] == "4.5.0" for r in out), 3)
        self.assertEqual(out[0], row(M42, "4.1.1", "4.4.1", "17"))

    def test_other_major_is_not_a_minor(self):
        m = mod("3.5", "3.5.9", "3.3.3", art="delta-spark_2.13", java="8")
        mods, _, adds = plan([m], [], delta={"delta-spark_2.13": ["3.3.3", "4.0.1", "4.4.1"]})
        self.assertEqual((mods[0]["delta"], adds), ("3.3.3", []))

    def test_default_becomes_the_per_spark_artifact_and_rows_carry_no_key(self):
        d = {"delta-spark_2.13": ["4.4.1", "4.5.0"], "delta-spark_4.2_2.13": ["4.4.1", "4.5.0"]}
        mods, out, _ = plan([m42()], rows42(), delta=d)
        self.assertEqual(mods[0]["delta_artifact"], "delta-spark_4.2_2.13")
        self.assertEqual(out[1], row(M42, "4.2.0", "4.5.0", "17", True))
        self.assertTrue(all("deltaArtifact" not in r for r in out))

    def test_per_spark_artifact_may_bring_a_new_major(self):
        d = {"delta-spark_4.2_2.13": ["4.4.1", "5.0.0"]}
        mods, _, adds = plan([m42()], rows42(), delta=d)
        self.assertEqual((mods[0]["delta"], adds), ("5.0.0", ["delta 5.0.0 (spark-4.2_2.13)"]))

    def test_default_artifact_switches_when_only_the_per_spark_one_has_the_version(self):
        d = {"delta-spark_2.13": ["4.4.1"], "delta-spark_4.2_2.13": ["4.4.1", "4.5.0"]}
        rows = rows42() + [row(M42, "4.2.0", "4.3.1", "17", deltaArtifact="delta-spark_2.13")]
        mods, out, _ = plan([m42()], rows, delta=d)
        self.assertEqual((mods[0]["delta"], mods[0]["delta_artifact"]), ("4.5.0", "delta-spark_4.2_2.13"))
        self.assertTrue(all("deltaArtifact" not in r for r in out if r["delta"] != "4.3.1"))
        old = [r for r in out if r["delta"] == "4.3.1"]
        self.assertEqual(old[0]["deltaArtifact"], "delta-spark_2.13")  # explicit older row keeps its artifact

    def test_inheriting_rows_missing_from_the_new_default_name_the_old_one(self):
        d = {"delta-spark_2.13": ["4.3.1", "4.4.1"], "delta-spark_4.2_2.13": ["4.4.1", "4.5.0"]}
        rows = [row(M42, "4.2.0", "4.3.1", "17"), row(M42, "4.2.0", "4.4.1", "17", True)]
        _, out, _ = plan([m42()], rows, delta=d)
        self.assertEqual(out[0], row(M42, "4.2.0", "4.3.1", "17", deltaArtifact="delta-spark_2.13"))

    def test_delta_patch_also_switches_the_default(self):
        d = {"delta-spark_2.13": ["4.4.1"], "delta-spark_4.2_2.13": ["4.4.1", "4.4.2"]}
        mods, out, _ = plan([m42()], rows42(), delta=d)
        self.assertEqual(mods[0]["delta_artifact"], "delta-spark_4.2_2.13")
        self.assertTrue(all("deltaArtifact" not in r and r["delta"] == "4.4.2" for r in out))

    def test_copy_drops_delta_artifact_key(self):
        r = [row(M42, "4.2.0", "4.4.1", "17", True, deltaArtifact="delta-spark_2.13")]
        _, out, _ = plan([m42()], r, delta=self.D)
        self.assertNotIn("deltaArtifact", out[-1])

    def test_rc_ignored(self):
        mods, _, _ = plan([m42()], rows42(), delta={"delta-spark_2.13": ["4.4.1", "4.5.0-rc1"]})
        self.assertEqual(mods[0]["delta"], "4.4.1")


class NewLineTest(unittest.TestCase):  # rule 5
    S = {"2.13": ["4.2.0", "4.3.0", "4.3.0-preview1"]}
    D = {"delta-spark_4.3_2.13": ["4.5.0-rc1", "4.5.0"]}

    def test_requires_delta_artifact(self):
        mods, out, adds = plan([m42()], rows42(), spark=self.S, delta={})
        self.assertEqual((len(mods), out, adds), (1, rows42(), []))

    def test_new_module_and_rows(self):
        mods, out, adds = plan([m42()], rows42(), spark=self.S, delta=self.D)
        new = mods[-1]
        self.assertEqual((new["minor"], new["spark"], new["delta"], new["java"], new["delta_artifact"]),
                         ("4.3", "4.3.0", "4.5.0", "17", "delta-spark_4.3_2.13"))
        self.assertEqual((new["scala_version"], new["hadoop"]), ("2.13.19", "3.5.0"))
        self.assertEqual(out[3:], [row("spark-4.3_2.13", "4.3.0", "4.5.0", j, s)
                                   for j, s in (("17", True), ("21", False), ("25", True))])
        self.assertEqual(adds, ["module spark-4.3_2.13 (spark 4.3.0, delta 4.5.0)"])

    def test_older_or_equal_lines_are_not_new(self):
        s = {"2.13": ["3.4.4", "4.1.3", "4.2.0"]}
        d = {"delta-spark_3.4_2.13": ["3.9.0"], "delta-spark_4.1_2.13": ["4.4.1"]}
        mods, _, adds = plan([m42()], rows42(), spark=s, delta=d)
        self.assertEqual((len(mods), adds), (1, []))

    def test_template_is_the_newest_scala_2_13_module(self):
        a = mod("3.5", "3.5.3", "3.3.3", scala="2.12", art="delta-spark_2.12", java="8")
        mods, _, _ = plan([a, m42()], rows42(), spark=self.S, delta=self.D)
        self.assertEqual((mods[-1]["minor"], mods[-1]["java"]), ("4.3", "17"))


class GapTest(unittest.TestCase):
    def test_missing_minor_between_modules_is_added_in_version_order(self):
        a = mod("4.0", "4.0.4", "4.4.1")
        ra = [row("spark-4.0_2.13", "4.0.4", "4.4.1", "17", True)]
        s = {"2.13": ["4.0.4", "4.1.3", "4.2.0"]}
        d = {"delta-spark_4.0_2.13": ["4.4.1"], "delta-spark_4.1_2.13": ["4.4.1"]}
        mods, out, adds = plan([a, m42()], ra + rows42(), spark=s, delta=d)
        self.assertEqual([m["minor"] for m in mods], ["4.0", "4.1", "4.2"])
        self.assertEqual([r["module"] for r in out][:2], ["spark-4.0_2.13", "spark-4.1_2.13"])
        self.assertEqual(adds, ["module spark-4.1_2.13 (spark 4.1.3, delta 4.4.1)"])

    def test_lines_below_the_oldest_per_spark_4x_module_are_ignored(self):
        a = mod("4.0", "4.0.4", "4.4.1")
        s = {"2.13": ["3.4.4", "4.0.4"]}
        d = {"delta-spark_4.0_2.13": ["4.4.1"], "delta-spark_3.4_2.13": ["3.9.0"]}
        _, _, adds = plan([a], [], spark=s, delta=d)
        self.assertEqual(adds, [])


class OrderTest(unittest.TestCase):  # rule 6
    def test_spark_patch_then_delta_patch_then_minor_then_module(self):
        rows = rows42()
        mods = [m42()]
        s = {"2.13": ["4.2.1", "4.3.0"]}
        d = {"delta-spark_2.13": ["4.4.1", "4.4.2", "4.5.0"], "delta-spark_4.3_2.13": ["4.5.0"]}
        _, out, adds = plan(mods, rows, spark=s, delta=d, par=parent("2.13.18", "3.4.1", old=("4.2.0", "4.2.1")))
        self.assertEqual(adds, ["spark 4.2.1 (spark-4.2_2.13)", "delta 4.4.2 (spark-4.2_2.13)",
                                "delta 4.5.0 (spark-4.2_2.13)", "module spark-4.3_2.13 (spark 4.3.0, delta 4.5.0)"])
        # the minor copied the patched rows (Spark 4.2.1, Delta 4.4.2), and the new module copied the 4.5.0 rows
        self.assertEqual([(r["module"], r["spark"], r["delta"]) for r in out[:2]],
                         [(M42, "4.2.1", "4.4.2"), (M42, "4.2.1", "4.5.0")])
        self.assertEqual(out[-1]["module"], "spark-4.3_2.13")

    def test_nothing_to_do_prints_nothing(self):
        _, _, adds = plan([m42()], rows42(), spark={"2.13": ["4.2.0"]}, delta={"delta-spark_2.13": ["4.4.1"]})
        self.assertEqual(adds, [])


POM = """<project>
  <artifactId>gluecat-parent</artifactId>
  <artifactId>gluecat-spark-4.2_2.13</artifactId>
  <name>gluecat for Spark 4.2 (Scala 2.13)</name>
  <description>AWS Glue Data Catalog client for Apache Spark 4.2, Scala 2.13</description>
  <properties>
    <scala.version>2.13.18</scala.version>
    <scala.binary.version>2.13</scala.binary.version>
    <spark.version>4.2.0</spark.version>
    <spark.minor>4.2</spark.minor>
    <delta.version>4.4.1</delta.version>
    <java.release>17</java.release>
    <hadoop.aws.version>3.4.1</hadoop.aws.version>
  </properties>
</project>
"""


class WriterTest(unittest.TestCase):
    def test_edit_pom_changes_one_line(self):
        out = discover.edit_pom(POM, "spark.version", "4.2.1")
        self.assertEqual(out, POM.replace("<spark.version>4.2.0<", "<spark.version>4.2.1<"))
        self.assertEqual(discover.edit_pom(POM, "spark.version", "4.2.0"), POM)

    def test_edit_pom_missing_property(self):
        with self.assertRaises(ValueError):
            discover.edit_pom(POM, "nope", "1")

    def test_new_module_pom(self):
        out = discover.new_module_pom(POM, "4.3", "4.3.0", "4.5.0", "2.13.19", "3.5.0")
        self.assertIn("<artifactId>gluecat-spark-4.3_2.13</artifactId>", out)
        self.assertIn("<name>gluecat for Spark 4.3 (Scala 2.13)</name>", out)
        self.assertIn("client for Apache Spark 4.3, Scala 2.13</description>", out)
        self.assertIn("<artifactId>gluecat-parent</artifactId>", out)
        for p, v in (("spark.version", "4.3.0"), ("spark.minor", "4.3"), ("delta.version", "4.5.0"),
                     ("scala.version", "2.13.19"), ("hadoop.aws.version", "3.5.0"), ("java.release", "17"),
                     ("delta.artifact", "delta-spark_4.3_2.13")):
            self.assertIn(f"<{p}>{v}</{p}>", out)
        self.assertNotIn("4.2", out.replace("2.13", ""))

    def test_new_module_pom_replaces_an_existing_delta_artifact(self):
        pom = POM.replace("<java.release>", "<delta.artifact>delta-spark_4.2_2.13</delta.artifact>\n    <java.release>")
        out = discover.new_module_pom(pom, "4.3", "4.3.0", "4.5.0", "2.13.19", "3.5.0")
        self.assertEqual(out.count("<delta.artifact>"), 1)
        self.assertIn("<delta.artifact>delta-spark_4.3_2.13</delta.artifact>", out)

    def test_add_module_after_the_last_one(self):
        text = "<modules>\n    <module>modules/a</module>\n    <module>modules/b</module>\n  </modules>\n"
        self.assertEqual(discover.add_module(text, "modules/c"),
                         "<modules>\n    <module>modules/a</module>\n    <module>modules/b</module>\n"
                         "    <module>modules/c</module>\n  </modules>\n")

    def test_changelog_creates_unreleased_before_the_first_release(self):
        out = discover.changelog_unreleased("# Changelog\n\n## 1.1.0 - 2026-10-24\n\n- x\n", "New thing.")
        self.assertEqual(out, "# Changelog\n\n## Unreleased\n\n- New thing.\n\n## 1.1.0 - 2026-10-24\n\n- x\n")

    def test_changelog_adds_to_an_existing_unreleased(self):
        text = "# Changelog\n\n## Unreleased\n\n- Old.\n\n## 1.1.0 - 2026-10-24\n"
        self.assertEqual(discover.changelog_unreleased(text, "New."),
                         "# Changelog\n\n## Unreleased\n\n- Old.\n- New.\n\n## 1.1.0 - 2026-10-24\n")

    def test_write_matrix_layout(self):
        rows = [row("a", "1.0.0", "2.0.0", "17", True, deltaArtifact="x"), row("b", "1.0.0", "2.0.0", "8")]
        self.assertEqual(discover.write_matrix(rows),
                         '[\n  {"module": "a", "spark": "1.0.0", "delta": "2.0.0", "deltaArtifact": "x", '
                         '"jdk": "17", "smoke": true},\n  {"module": "b", "spark": "1.0.0", "delta": "2.0.0", '
                         '"jdk": "8", "smoke": false}\n]\n')

    def test_write_matrix_is_byte_identical_on_the_real_file(self):
        self.assertEqual(discover.write_matrix(render_tables.read_rows(ROOT)), (ROOT / "matrix.json").read_text())


class ParseTest(unittest.TestCase):
    def test_versions(self):
        xml = ("<metadata><versioning><versions><version>1.0.0</version><version>1.1.0-rc1</version>"
               "</versions></versioning></metadata>")
        self.assertEqual(discover.parse_versions(xml), ["1.0.0", "1.1.0-rc1"])

    def test_spark_parent_reads_top_level_properties_not_profiles(self):
        pom = ('<project xmlns="http://maven.apache.org/POM/4.0.0"><profiles><profile><properties>'
               "<scala.version>2.12.18</scala.version></properties></profile></profiles>"
               "<properties><hadoop.version>3.3.4</hadoop.version><scala.version>2.13.8</scala.version>"
               "</properties></project>")
        self.assertEqual(discover.parse_spark_parent(pom), {"hadoop": "3.3.4", "scala": "2.13.8"})


def fake_current():
    mods = render_tables.read_modules(FIXTURE)
    out = {}
    for m in mods:
        out.setdefault(f"org.apache.spark:spark-sql_{m['scala']}", []).append(m["spark"])
        out.setdefault(f"io.delta:{m['delta_artifact']}", []).append(m["delta"])
    for r in render_tables.read_rows(FIXTURE):
        if "deltaArtifact" in r:
            out.setdefault(f"io.delta:{r['deltaArtifact']}", []).append(r["delta"])
    return out


def copy_repo(dst):
    shutil.copy(FIXTURE / "pom.xml", dst / "pom.xml")
    shutil.copy(FIXTURE / "matrix.json", dst / "matrix.json")
    shutil.copy(FIXTURE / "CHANGELOG.md", dst / "CHANGELOG.md")
    for p in (FIXTURE / "modules").glob("*/pom.xml"):
        (dst / "modules" / p.parent.name).mkdir(parents=True)
        shutil.copy(p, dst / "modules" / p.parent.name / "pom.xml")


def snapshot(d):
    return {str(p.relative_to(d)): p.read_bytes() for p in sorted(d.rglob("*")) if p.is_file()}


def run_main(root, fake, *args):
    out = io.StringIO()
    with mock.patch.dict(os.environ, {"DISCOVER_FAKE_MAVEN": json.dumps(fake)}), contextlib.redirect_stdout(out):
        code = discover.main(list(args), root=root)
    return code, out.getvalue()


class EndToEndTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        copy_repo(self.root)

    def tearDown(self):
        self.tmp.cleanup()

    def test_no_change_writes_nothing_and_prints_nothing(self):
        before = snapshot(self.root)
        code, out = run_main(self.root, fake_current())
        self.assertEqual((code, out), (0, ""))
        self.assertEqual(snapshot(self.root), before)

    def test_dry_run_prints_but_does_not_write(self):
        fake = fake_current()
        fake["org.apache.spark:spark-sql_2.13"].append("4.2.1")
        fake["spark-parent:2.13:4.2.0"] = {"hadoop": "3.4.1", "scala": "2.13.18"}
        fake["spark-parent:2.13:4.2.1"] = {"hadoop": "3.5.0", "scala": "2.13.19"}
        before = snapshot(self.root)
        code, out = run_main(self.root, fake, "--dry-run")
        self.assertEqual(out, "spark 4.2.1 (spark-4.2_2.13: scala 2.13.19, hadoop-aws 3.5.0)\n")
        self.assertEqual(snapshot(self.root), before)

    def test_spark_patch_edits_only_the_expected_lines(self):
        fake = fake_current()
        fake["org.apache.spark:spark-sql_2.13"].append("4.2.1")
        fake["spark-parent:2.13:4.2.0"] = {"hadoop": "3.4.1", "scala": "2.13.18"}
        fake["spark-parent:2.13:4.2.1"] = {"hadoop": "3.5.0", "scala": "2.13.19"}
        before = snapshot(self.root)
        run_main(self.root, fake)
        after = snapshot(self.root)
        changed = [k for k in after if after[k] != before[k]]
        self.assertEqual(sorted(changed), ["matrix.json", "modules/spark-4.2_2.13/pom.xml"])
        pom = after["modules/spark-4.2_2.13/pom.xml"].decode()
        self.assertIn("<spark.version>4.2.1</spark.version>", pom)
        self.assertIn("<hadoop.aws.version>3.5.0</hadoop.aws.version>", pom)

    def test_default_artifact_switch_adds_the_property_to_the_pom(self):
        fake = fake_current()
        fake["io.delta:delta-spark_4.2_2.13"] = ["4.4.1", "4.5.0"]
        run_main(self.root, fake)
        pom = (self.root / "modules/spark-4.2_2.13/pom.xml").read_text()
        self.assertIn("<delta.version>4.5.0</delta.version>\n    <delta.artifact>delta-spark_4.2_2.13"
                      "</delta.artifact>\n", pom)
        self.assertEqual(render_tables.read_modules(self.root)[4]["delta_artifact"], "delta-spark_4.2_2.13")

    def test_new_line_writes_module_parent_matrix_and_changelog(self):
        fake = fake_current()
        fake["org.apache.spark:spark-sql_2.13"].append("4.3.0")
        fake["io.delta:delta-spark_4.3_2.13"] = ["4.5.0"]
        fake["spark-parent:2.13:4.3.0"] = {"hadoop": "3.5.0", "scala": "2.13.19"}
        code, out = run_main(self.root, fake)
        self.assertIn("module spark-4.3_2.13 (spark 4.3.0, delta 4.5.0)", out)
        self.assertTrue((self.root / "modules/spark-4.3_2.13/pom.xml").exists())
        self.assertIn("<module>modules/spark-4.3_2.13</module>", (self.root / "pom.xml").read_text())
        self.assertIn("## Unreleased", (self.root / "CHANGELOG.md").read_text())
        self.assertIn("spark-4.3_2.13", (self.root / "CHANGELOG.md").read_text())
        mods = render_tables.read_modules(self.root)
        self.assertEqual(mods[-1]["minor"], "4.3")

    def test_two_new_lines_in_one_run(self):
        fake = fake_current()
        fake["org.apache.spark:spark-sql_2.13"] += ["4.3.0", "4.4.0"]
        fake["io.delta:delta-spark_4.3_2.13"] = ["4.5.0"]
        fake["io.delta:delta-spark_4.4_2.13"] = ["4.6.0"]
        fake["spark-parent:2.13:4.3.0"] = {"hadoop": "3.5.0", "scala": "2.13.19"}
        fake["spark-parent:2.13:4.4.0"] = {"hadoop": "3.5.1", "scala": "2.13.19"}
        code, out = run_main(self.root, fake)
        self.assertEqual(out.splitlines(), ["module spark-4.3_2.13 (spark 4.3.0, delta 4.5.0)",
                                            "module spark-4.4_2.13 (spark 4.4.0, delta 4.6.0)"])
        mods = render_tables.read_modules(self.root)
        self.assertEqual([m["minor"] for m in mods[-2:]], ["4.3", "4.4"])
        self.assertEqual(mods[-1]["hadoop"], "3.5.1")
        self.assertEqual(mods[-1]["delta"], "4.6.0")
        log = (self.root / "CHANGELOG.md").read_text()
        self.assertIn("gluecat-spark-4.3_2.13", log)
        self.assertIn("gluecat-spark-4.4_2.13", log)
        self.assertEqual(run_main(self.root, fake)[1].count("module "), 0)  # nothing left to do

    def test_a_failure_while_computing_writes_nothing(self):
        fake = fake_current()
        fake["org.apache.spark:spark-sql_2.13"] += ["4.3.0"]
        fake["io.delta:delta-spark_4.3_2.13"] = ["4.5.0"]  # no spark-parent entry: the run exits
        before = snapshot(self.root)
        with self.assertRaises(SystemExit):
            run_main(self.root, fake)
        self.assertEqual(snapshot(self.root), before)

    def test_network_failure_exits_non_zero(self):
        with mock.patch.object(discover, "RETRY_SECONDS", 0), \
                mock.patch("urllib.request.urlopen", side_effect=OSError("down")), \
                mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("DISCOVER_FAKE_MAVEN", None)
            with mock.patch("urllib.request.urlopen", side_effect=OSError("down")) as urlopen:
                with self.assertRaises(SystemExit) as cm:
                    discover.fetch_versions("io.delta", "delta-spark_2.13")
            self.assertEqual(urlopen.call_count, 2)
            self.assertNotEqual(cm.exception.code, 0)

    def test_404_means_the_artifact_does_not_exist(self):
        err = urllib.error.HTTPError("u", 404, "Not Found", {}, None)
        with mock.patch.dict(os.environ, {}), mock.patch("urllib.request.urlopen", side_effect=err) as urlopen:
            os.environ.pop("DISCOVER_FAKE_MAVEN", None)
            self.assertEqual(discover.fetch_versions("io.delta", "delta-spark_4.9_2.13"), [])
            self.assertEqual(urlopen.call_count, 1)

    def test_spark_parent_values_are_validated(self):
        for bad in ({"hadoop": "3.5.0-SNAPSHOT", "scala": "2.13.19"}, {"hadoop": "3.5.0", "scala": "2.12.18"},
                    {"hadoop": "${hadoop.version}", "scala": "2.13.19"}):
            with mock.patch.dict(os.environ, {"DISCOVER_FAKE_MAVEN": json.dumps({"spark-parent:2.13:4.3.0": bad})}):
                with self.assertRaises(SystemExit):
                    discover.fetch_spark_parent("2.13", "4.3.0")

class PerModuleNotesTest(unittest.TestCase):
    def test_notes_name_the_module_they_belong_to(self):
        a = mod("3.5", "3.5.9", "3.3.3", scala="2.12", sv="2.12.18", hadoop="3.3.4", art="delta-spark_2.12")
        b = mod("3.5", "3.5.9", "3.3.3", scala="2.13", sv="2.13.8", hadoop="3.3.4", art="delta-spark_2.13")
        rows = [row("spark-3.5_2.12", "3.5.9", "3.3.3", "8"), row("spark-3.5_2.13", "3.5.9", "3.3.3", "8")]

        def par(scala, version):
            if version == "3.5.9":
                return {"scala": "2.12.18" if scala == "2.12" else "2.13.8", "hadoop": "3.3.4"}
            return {"scala": "2.12.22" if scala == "2.12" else "2.13.8", "hadoop": "3.3.6"}

        _, _, adds = plan([a, b], rows, spark={"2.12": ["3.5.10"], "2.13": ["3.5.10"]}, par=par)
        self.assertEqual(adds, ["spark 3.5.10 (spark-3.5_2.12: scala 2.12.22, hadoop-aws 3.3.6; "
                                "spark-3.5_2.13: hadoop-aws 3.3.6)"])


class InvariantRulesTest(unittest.TestCase):
    def test_delta_minor_on_a_module_without_current_rows_raises(self):
        stale = [row(M42, "4.2.0", "4.3.0", "17")]
        with self.assertRaisesRegex(ValueError, "spark-4.2_2.13: no matrix.json row"):
            plan([m42()], stale, delta={"delta-spark_2.13": ["4.4.1", "4.5.0"]})

    def test_new_line_whose_predecessor_has_no_current_rows_raises(self):
        stale = [row(M42, "4.2.0", "4.3.0", "17")]
        with self.assertRaisesRegex(ValueError, "spark-4.2_2.13: no matrix.json row"):
            plan([m42()], stale, spark={"2.13": ["4.2.0", "4.3.0"]}, delta={"delta-spark_4.3_2.13": ["4.5.0"]})

    def test_main_refuses_a_plan_that_breaks_the_invariant(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            copy_repo(root)
            rows = render_tables.read_rows(root)
            (root / "matrix.json").write_text(discover.write_matrix([r for r in rows if r["module"] != M42]))
            fake = fake_current()
            fake["org.apache.spark:spark-sql_2.13"].append("4.2.1")
            fake["spark-parent:2.13:4.2.0"] = fake["spark-parent:2.13:4.2.1"] = {"hadoop": "3.4.1", "scala": "2.13.18"}
            before = snapshot(root)
            with self.assertRaises(SystemExit):
                run_main(root, fake)
            self.assertEqual(snapshot(root), before)

    def test_live_tree_satisfies_the_invariant(self):
        render_tables.validate(render_tables.read_modules(ROOT), render_tables.read_rows(ROOT))


class IgnoreTest(unittest.TestCase):
    def test_parse(self):
        text = "# comment\n\nspark 4.3  # trailing\ndelta 4.5.0 spark-4.0_2.13\nmodule spark-4.3_2.13\n"
        self.assertEqual(discover.parse_ignore(text), [("spark", "4.3", None), ("delta", "4.5.0", "spark-4.0_2.13"),
                                                       ("module", "spark-4.3_2.13", None)])

    def test_bad_line_names_its_number(self):
        for bad in ("nope 1", "spark", "module a b", "delta 1 a b"):
            with self.assertRaisesRegex(ValueError, "line 2"):
                discover.parse_ignore("# ok\n" + bad + "\n")

    def test_spark_patch_is_skipped_and_the_next_one_used(self):
        ign = discover.parse_ignore("spark 4.2.1")
        mods, _, adds = plan_i([m42()], rows42(), ign, spark={"2.13": ["4.2.1", "4.2.2"]})
        self.assertEqual(mods[0]["spark"], "4.2.2")
        self.assertTrue(adds[0].startswith("spark 4.2.2 (spark-4.2_2.13"))

    def test_whole_spark_line_is_skipped(self):
        ign = discover.parse_ignore("spark 4.3")
        mods, _, adds = plan_i([m42()], rows42(), ign, spark={"2.13": ["4.2.0", "4.3.0", "4.3.1"]},
                               delta={"delta-spark_4.3_2.13": ["4.5.0"]})
        self.assertEqual((len(mods), adds), (1, []))

    def test_spark_line_prefix_does_not_match_a_longer_minor(self):
        self.assertFalse(discover.declined([("spark", "4.1", None)], "spark", "4.10.0"))
        self.assertTrue(discover.declined([("spark", "4.1", None)], "spark", "4.1.3"))

    def test_delta_patch_and_minor_are_skipped_everywhere(self):
        ign = discover.parse_ignore("delta 4.5.0")
        mods, out, adds = plan_i([m42()], rows42(), ign, delta={"delta-spark_2.13": ["4.4.1", "4.5.0"]})
        self.assertEqual((mods[0]["delta"], adds, out), ("4.4.1", [], rows42()))

    def test_delta_entry_with_a_module_skips_only_that_module(self):
        other = mod("4.1", "4.1.3", "4.4.1", art="delta-spark_2.13")
        rows = [row("spark-4.1_2.13", "4.1.3", "4.4.1", "17")] + rows42()
        ign = discover.parse_ignore("delta 4.4.2 spark-4.2_2.13")
        mods, _, adds = plan_i([other, m42()], rows, ign, delta={"delta-spark_2.13": ["4.4.1", "4.4.2"]})
        self.assertEqual([m["delta"] for m in mods], ["4.4.2", "4.4.1"])
        self.assertEqual(adds, ["delta 4.4.2 (spark-4.1_2.13)"])

    def test_module_entry_blocks_only_the_module(self):
        s, d = {"2.13": ["4.2.0", "4.3.0"]}, {"delta-spark_4.3_2.13": ["4.5.0"]}
        self.assertEqual(plan_i([m42()], rows42(), [("module", "spark-4.3_2.13", None)], spark=s, delta=d)[2], [])
        self.assertEqual(len(plan_i([m42()], rows42(), [], spark=s, delta=d)[2]), 1)

    def test_delta_entry_blocks_the_new_module_version(self):
        s, d = {"2.13": ["4.2.0", "4.3.0"]}, {"delta-spark_4.3_2.13": ["4.5.0"]}
        self.assertEqual(plan_i([m42()], rows42(), [("delta", "4.5.0", None)], spark=s, delta=d)[2], [])

    def test_main_reads_the_file_next_to_the_scripts(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            copy_repo(root)
            (root / "scripts").mkdir()
            (root / "scripts" / "discover-ignore.txt").write_text("spark 4.2.1  # not yet\n")
            fake = fake_current()
            fake["org.apache.spark:spark-sql_2.13"].append("4.2.1")
            before = snapshot(root)
            self.assertEqual(run_main(root, fake), (0, ""))
            self.assertEqual(snapshot(root), before)


def plan_i(modules, rows, ignore, spark=None, delta=None):
    return discover.plan_updates(modules, rows, spark or {}, delta or {}, parent(), ignore)


class LiveTreeTest(unittest.TestCase):
    """State-independent checks that run against the real files."""

    def test_nothing_new_changes_nothing(self):
        before = snapshot(ROOT / "modules")
        self.assertEqual(run_main(ROOT, {}, "--dry-run"), (0, ""))
        self.assertEqual(snapshot(ROOT / "modules"), before)


if __name__ == "__main__":
    unittest.main()
