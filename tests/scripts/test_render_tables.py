import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import render_tables  # noqa: E402


def mod(minor, scala, spark, java="17"):
    return {"minor": minor, "scala": scala, "spark": spark, "java": java}


def row(module, spark, delta, jdk):
    return {"module": module, "spark": spark, "delta": delta, "jdk": jdk}


def line(modules, rows, i=0):
    return render_tables.table(modules, rows).splitlines()[2 + i]


class TableTest(unittest.TestCase):
    def test_header(self):
        out = render_tables.table([], []).splitlines()
        self.assertEqual(out[0], "| Artifact | Spark | Scala | Java bytecode | JDKs tested | Delta tested |")
        self.assertEqual(out[1], "|---|---|---|---|---|---|")

    def test_floor_patch_is_and_later(self):
        m = [mod("3.5", "2.12", "3.5.9", "8")]
        r = [row("spark-3.5_2.12", "3.5.3", "3.3.3", "8"), row("spark-3.5_2.12", "3.5.9", "3.3.3", "8"),
             row("spark-3.5_2.12", "3.5.9", "3.3.3", "17"), row("spark-3.5_2.12", "3.5.9", "3.3.3", "11")]
        self.assertEqual(line(m, r), "| `gluecat-spark-3.5_2.12` | 3.5.3 and later 3.5 | 2.12 | 8 | 8, 11, 17 | 3.3.3 |")

    def test_single_spark_is_x(self):
        m = [mod("4.2", "2.13", "4.2.0")]
        r = [row("spark-4.2_2.13", "4.2.0", "4.4.1", j) for j in ("25", "17", "21")]
        self.assertEqual(line(m, r), "| `gluecat-spark-4.2_2.13` | 4.2.x | 2.13 | 17 | 17, 21, 25 | 4.4.1 |")

    def test_pinned_delta_note_and_version_order(self):
        m = [mod("4.1", "2.13", "4.1.3")]
        r = [row("spark-4.1_2.13", "4.1.1", "4.1.0", "17"), row("spark-4.1_2.13", "4.1.1", "4.2.0", "17"),
             row("spark-4.1_2.13", "4.1.3", "4.3.1", "17"), row("spark-4.1_2.13", "4.1.3", "4.4.1", "21")]
        self.assertEqual(
            line(m, r),
            "| `gluecat-spark-4.1_2.13` | 4.1.x | 2.13 | 17 | 17, 21 | 4.1.0 and 4.2.0 (on Spark 4.1.1), 4.3.1, 4.4.1 |")

    def test_delta_numeric_order(self):
        m = [mod("4.0", "2.13", "4.0.4")]
        r = [row("spark-4.0_2.13", "4.0.4", d, "17") for d in ("4.10.0", "4.9.1", "4.4.1")]
        self.assertTrue(line(m, r).endswith("| 4.4.1, 4.9.1, 4.10.0 |"))

    def test_module_order_and_rows_by_module(self):
        m = [mod("4.0", "2.13", "4.0.4"), mod("4.1", "2.13", "4.1.3")]
        r = [row("spark-4.1_2.13", "4.1.3", "4.4.1", "21"), row("spark-4.0_2.13", "4.0.4", "4.0.1", "17")]
        self.assertIn("gluecat-spark-4.0_2.13", line(m, r, 0))
        self.assertIn("gluecat-spark-4.1_2.13", line(m, r, 1))


class ReplaceTest(unittest.TestCase):
    def test_replace_between_markers(self):
        text = "a\n<!-- artifacts:start -->\nold\n<!-- artifacts:end -->\nb\n"
        self.assertEqual(render_tables.replace_block(text, "NEW"),
                         "a\n<!-- artifacts:start -->\nNEW\n<!-- artifacts:end -->\nb\n")

    def test_missing_markers_fail(self):
        with self.assertRaises(ValueError):
            render_tables.replace_block("no markers", "x")


class ReadTest(unittest.TestCase):
    def test_read_modules_in_parent_order(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / "pom.xml").write_text(
                "<modules>\n<module>modules/b</module>\n<module>modules/a</module>\n</modules>"
                "<delta.artifact>delta-spark_${scala.binary.version}</delta.artifact>")
            for name, minor in (("a", "3.5"), ("b", "4.0")):
                (root / "modules" / name).mkdir(parents=True)
                (root / "modules" / name / "pom.xml").write_text(
                    f"<scala.binary.version>2.13</scala.binary.version><spark.version>{minor}.1</spark.version>"
                    f"<spark.minor>{minor}</spark.minor><delta.version>1</delta.version>"
                    "<java.release>17</java.release><scala.version>2.13.1</scala.version>"
                    "<hadoop.aws.version>3.4.1</hadoop.aws.version>")
            mods = render_tables.read_modules(root)
            self.assertEqual([m["minor"] for m in mods], ["4.0", "3.5"])
            self.assertEqual(mods[0]["spark"], "4.0.1")
            self.assertEqual(mods[0]["delta_artifact"], "delta-spark_2.13")

    def test_missing_property_names_the_file(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / "pom.xml").write_text("<module>m</module><delta.artifact>x</delta.artifact>")
            (root / "m").mkdir()
            (root / "m" / "pom.xml").write_text("<spark.version>1.0.0</spark.version>")
            with self.assertRaisesRegex(ValueError, r"pom\.xml: missing <spark\.minor>"):
                render_tables.read_modules(root)


if __name__ == "__main__":
    unittest.main()
