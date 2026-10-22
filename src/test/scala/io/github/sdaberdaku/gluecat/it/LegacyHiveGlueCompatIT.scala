package io.github.sdaberdaku.gluecat.it

import scala.collection.JavaConverters._
import scala.sys.process.{Process, ProcessLogger}

import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import io.github.sdaberdaku.gluecat.catalog.Fixtures
import io.github.sdaberdaku.gluecat.glue.GlueColumn

/** Spark 3.5 on its Hive path with the Glue Hive client (the legacy-spark container) and gluecat read each other's
  * tables in the same MiniStack store. Needs the containers in docker/docker-compose.yml and LEGACY_SPARK_CONTAINER;
  * without it every test is cancelled. */
abstract class LegacyHiveGlueCompatIT(mode: Mode) extends SparkIT(mode) {
  private val container = sys.env.get("LEGACY_SPARK_CONTAINER")
  private def needsLegacy(): Unit =
    assume(container.isDefined, "LEGACY_SPARK_CONTAINER is not set (make compat-up, then make compat)")

  /** Runs `sql` with the legacy Spark's spark-sql (configured by docker/legacy/spark-defaults.conf) and returns the
    * lines it prints; a non-zero exit fails the test with the output. */
  private def legacy(sql: String): Seq[String] = {
    val out = new StringBuilder
    val err = new StringBuilder
    val exit = Process(Seq("docker", "exec", container.get, "spark-sql", "-S", "-e", sql))
      .!(ProcessLogger(l => out.append(l).append('\n'), l => err.append(l).append('\n')))
    assert(exit == 0, s"legacy spark-sql exited with $exit on: $sql\n$err$out")
    out.toString.split('\n').filter(_.nonEmpty).toList
  }

  private def rows(sql: String): Seq[(Int, String)] =
    spark.sql(sql).collect().map(r => (r.getInt(0), r.getString(1))).toSeq
  private def describedColumns(t: String): Seq[String] =
    spark.sql(s"DESCRIBE $t").collect().map(_.getString(0)).take(2).toSeq

  // Created on first use, after the assume: /tmp/gluecat-compat exists only while the containers run. The database
  // gets a location because Spark's Hive path cannot create tables in a Glue database without one (the Glue client
  // turns the missing LocationUri into "", and HiveExternalCatalog.getDatabase fails on the empty path).
  private lazy val dir = {
    val d = compatDir()
    spark.sql(s"CREATE DATABASE $cat.db LOCATION '$d/db'")
    d
  }

  test("legacy Spark reads gluecat tables") {
    needsLegacy()
    spark.sql(s"CREATE TABLE ${tbl("db", "t")} (id INT, name STRING) USING delta LOCATION '$dir/t'")
    spark.sql(s"INSERT INTO ${tbl("db", "t")} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    spark.sql(s"CREATE TABLE ${tbl("db", "p")} USING delta PARTITIONED BY (dt) LOCATION '$dir/p' " +
      "AS SELECT 1 AS id, '2026-10-22' AS dt UNION ALL SELECT 2, '2026-10-23'")
    // One spark-sql run; DESCRIBE goes last because its length varies.
    val out = legacy("SELECT count(*) FROM db.t; SELECT id, name FROM db.t ORDER BY id; " +
      "SELECT id FROM db.p WHERE dt = '2026-10-22'; DESCRIBE db.t")
    assert(out.take(5) == Seq("3", "1\ta", "2\tb", "3\tc", "1"))
    assert(out.drop(5).map(_.split('\t').head.trim).take(2) == Seq("id", "name"))
  }

  test("gluecat reads legacy tables") {
    needsLegacy()
    legacy(s"CREATE TABLE db.legacy USING delta LOCATION '$dir/legacy' AS SELECT 1 AS id, 'a' AS name; " +
      s"CREATE TABLE db.legacy_parquet (id INT, name STRING) USING parquet LOCATION '$dir/legacy_parquet'; " +
      s"INSERT OVERWRITE DIRECTORY '$dir/legacy_parquet' USING parquet SELECT 1 AS id, 'a' AS name")
    for (t <- Seq("legacy", "legacy_parquet")) {
      assert(rows(s"SELECT id, name FROM ${tbl("db", t)}") == Seq((1, "a")), t)
      assert(describedColumns(tbl("db", t)) == Seq("id", "name"), t)
    }
  }

  // The fixture models this entry from a production copy; here the real producer writes it.
  test("the legacy Delta CTAS entry has the fixture's shape") {
    needsLegacy()
    val fixture = Fixtures.table("delta-spark35-ctas")
    val entry = glue.getTable("db", "legacy")
    val sd = entry.storageDescriptor()
    val params = entry.parameters().asScala.toMap
    val varying = Set("spark.sql.create.version", "transient_lastDdlTime")
    assert(entry.tableType() == fixture.tableType.get)
    assert(params.keySet == fixture.parameters.keySet)
    assert(params -- varying == fixture.parameters -- varying)
    assert(entry.partitionKeys().isEmpty && fixture.partitionKeys.isEmpty)
    assert(sd.columns().asScala.map(c => GlueColumn(c.name(), c.`type`(), Option(c.comment()))).toList ==
      fixture.storage.columns)
    assert((Option(sd.inputFormat()), Option(sd.outputFormat()), Option(sd.serdeInfo().serializationLibrary())) ==
      ((fixture.storage.inputFormat, fixture.storage.outputFormat, fixture.storage.serdeLibrary)))
    assert(sd.serdeInfo().parameters().asScala.toMap ==
      fixture.storage.serdeParameters + ("path" -> s"$dir/legacy"))
    // Spark's Hive path puts the placeholder under the database location, not next to the table's path.
    assert(sd.location() == s"$dir/db/legacy-__PLACEHOLDER__")
    assert(fixture.storage.location.exists(_.endsWith("-__PLACEHOLDER__")))
  }
}

@RunWith(classOf[JUnitRunner])
class LegacyHiveGlueCompatNamedIT extends LegacyHiveGlueCompatIT(Named)

@RunWith(classOf[JUnitRunner])
class LegacyHiveGlueCompatSessionIT extends LegacyHiveGlueCompatIT(Session)
