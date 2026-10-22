package io.github.sdaberdaku.gluecat.it

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

import io.delta.tables.DeltaTable
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

/** Everything a user does with a Delta table by name once it exists. */
abstract class DeltaOperationsIT(mode: Mode) extends SparkIT(mode) {
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"CREATE DATABASE $cat.db")
  }

  private def path(loc: String): File = new File(new URI(loc).getPath)
  private def hasDeltaLog(loc: String): Boolean = new File(path(loc), "_delta_log").isDirectory
  private def columns(t: String): Seq[String] =
    glue.getTable("db", t).storageDescriptor().columns().asScala.map(_.name()).toList
  private def version(t: String): Long = glue.getTable("db", t).versionId().toLong
  private def location(t: String): String = glue.getTable("db", t).storageDescriptor().location()
  private def param(t: String, k: String): String = glue.getTable("db", t).parameters().get(k)
  private def ids(t: String): Seq[Int] = spark.sql(s"SELECT id FROM ${tbl("db", t)} ORDER BY id").collect().map(_.getInt(0)).toSeq
  private def create(t: String, ddl: String = "id INT, name STRING"): String = {
    val loc = tempDir()
    spark.sql(s"CREATE TABLE ${tbl("db", t)} ($ddl) USING delta LOCATION '$loc'")
    loc
  }

  test("managed Delta table gets its location from the database location") {
    val dbDir = tempDir()
    spark.sql(s"CREATE DATABASE $cat.mdb LOCATION '$dbDir'")
    spark.sql(s"CREATE TABLE $cat.mdb.m (id INT) USING delta")
    val loc = glue.getTable("mdb", "m").storageDescriptor().location()
    assert(loc == s"$dbDir/m")
    assert(hasDeltaLog(loc))
    spark.sql(s"DROP TABLE $cat.mdb.m")
    assert(hasDeltaLog(loc))
    assert(!glue.listTableNames("mdb").contains("m"))
  }

  // Called on a catalog of its own: the fixture's catalog has a warehouse.
  test("managed table without any location fails") {
    glue.createDatabase("nowh")
    val catalog = new TestGlueCatalog()
    catalog.initialize("direct", new CaseInsensitiveStringMap(Map("glue.id" -> "111111111111",
      "client.region" -> "eu-central-1", "glue.endpoint" -> MiniStack.Endpoint).asJava))
    val e = intercept[IllegalArgumentException](catalog.createTable(Identifier.of(Array("nowh"), "m"),
      new StructType().add("id", IntegerType), Array.empty[Transform], Map("provider" -> "delta").asJava))
    assert(e.getMessage.contains("warehouse"))
    assert(!glue.listTableNames("nowh").contains("m"))
  }

  test("REPLACE TABLE AS SELECT changes the schema and the Glue columns, keeps unknown parameters") {
    val loc = create("rep")
    spark.sql(s"INSERT INTO ${tbl("db", "rep")} VALUES (1, 'a')")
    glue.setParameter("db", "rep", "other_engine", "kept")
    val before = version("rep")
    spark.sql(s"REPLACE TABLE ${tbl("db", "rep")} USING delta AS SELECT 5 AS id, 1.5D AS score")
    assert(columns("rep") == Seq("id", "score"))
    assert(param("rep", "spark.sql.sources.schema").contains("score"))
    assert(version("rep") > before)
    assert(param("rep", "other_engine") == "kept")
    assert(location("rep") == loc)
    assert(ids("rep") == Seq(5))
  }

  test("saveAsTable with mode overwrite on an existing name replaces the data") {
    val loc = create("sat")
    spark.sql(s"INSERT INTO ${tbl("db", "sat")} VALUES (1, 'a'), (2, 'b')")
    frame(Seq((7, "z")))("id", "name").write.format("delta").mode("overwrite").saveAsTable(tbl("db", "sat"))
    assert(ids("sat") == Seq(7))
    assert(location("sat") == loc)
  }

  test("CREATE OR REPLACE TABLE on a missing table creates it") {
    val loc = tempDir()
    spark.sql(s"CREATE OR REPLACE TABLE ${tbl("db", "cor")} USING delta LOCATION '$loc' AS SELECT 1 AS id")
    assert(hasDeltaLog(loc))
    assert(columns("cor") == Seq("id"))
    assert(ids("cor") == Seq(1))
  }

  test("UPDATE, DELETE, OPTIMIZE, VACUUM, DESCRIBE HISTORY and time travel work by name") {
    val t = tbl("db", "ops")
    create("ops")
    spark.sql(s"INSERT INTO $t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    val before = version("ops")
    spark.sql(s"UPDATE $t SET name = 'B' WHERE id = 2")
    spark.sql(s"DELETE FROM $t WHERE id = 3")
    assert(spark.sql(s"SELECT name FROM $t WHERE id = 2").head.getString(0) == "B")
    assert(ids("ops") == Seq(1, 2))
    spark.sql(s"OPTIMIZE $t")
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    try spark.sql(s"VACUUM $t RETAIN 0 HOURS") finally spark.conf.unset("spark.databricks.delta.retentionDurationCheck.enabled")
    val history = spark.sql(s"DESCRIBE HISTORY $t")
    assert(history.count() >= 2)
    // version 0 is the CREATE TABLE (empty), version 1 the INSERT
    assert(spark.sql(s"SELECT * FROM $t VERSION AS OF 0").count() == 0)
    assert(spark.sql(s"SELECT * FROM $t VERSION AS OF 1").count() == 3)
    val ts = history.where("version = 1").select("timestamp").head.getTimestamp(0)
    assert(spark.sql(s"SELECT * FROM $t TIMESTAMP AS OF '$ts'").count() == 3)
    assert(version("ops") == before) // none of this touches Glue
  }

  test("ALTER TABLE SET TBLPROPERTIES lands in the Delta log only") {
    val t = tbl("db", "props")
    create("props")
    val before = version("props")
    spark.sql(s"ALTER TABLE $t SET TBLPROPERTIES ('my.key' = 'v')")
    val shown = spark.sql(s"SHOW TBLPROPERTIES $t").collect().map(r => r.getString(0) -> r.getString(1)).toMap
    assert(shown.get("my.key").contains("v"))
    assert(version("props") == before)
  }

  test("ALTER TABLE ADD COLUMNS updates the Glue columns and the stored schema") {
    val t = tbl("db", "addc")
    create("addc")
    val before = version("addc")
    spark.sql(s"ALTER TABLE $t ADD COLUMNS (extra STRING)")
    assert(spark.sql(s"SELECT extra FROM $t").count() == 0)
    assert(columns("addc") == Seq("id", "name", "extra"))
    assert(param("addc", "spark.sql.sources.schema").contains("extra"))
    assert(version("addc") > before)
  }

  test("REFRESH TABLE re-syncs the Glue columns after a path-only schema change") {
    val loc = create("r", "id INT")
    glue.setParameter("db", "r", "other_engine", "kept")
    val before = version("r")
    spark.read.format("delta").load(loc).withColumn("extra", lit("x")).write.format("delta")
      .option("mergeSchema", "true").mode("append").save(loc)
    assert(columns("r") == Seq("id"))
    assert(version("r") == before)
    spark.sql(s"REFRESH TABLE ${tbl("db", "r")}")
    assert(columns("r") == Seq("id", "extra"))
    assert(param("r", "spark.sql.sources.schema").contains("extra"))
    assert(version("r") > before)
    assert(param("r", "other_engine") == "kept")
    assert(spark.sql(s"SELECT extra FROM ${tbl("db", "r")}").count() == 0)
  }

  // Spark 4.1's Catalog.refreshTable only drops the session's caches and never calls TableCatalog.invalidateTable,
  // so only the SQL statement re-syncs. The README promises REFRESH TABLE only.
  test("spark.catalog.refreshTable does not re-sync the Glue columns") {
    val loc = create("r2", "id INT")
    spark.read.format("delta").load(loc).withColumn("extra", lit("x")).write.format("delta")
      .option("mergeSchema", "true").mode("append").save(loc)
    assert(columns("r2") == Seq("id"))
    spark.catalog.refreshTable(tbl("db", "r2"))
    info(s"synced=${columns("r2")}")
    assert(columns("r2") == Seq("id"))
  }

  test("ALTER TABLE SET LOCATION to another Delta table moves the entry and re-syncs the columns") {
    val t = tbl("db", "mv")
    create("mv", "id INT")
    val other = tempDir()
    DeltaTable.create(spark).location(other).addColumn("id", "INT").addColumn("other", "STRING").execute()
    val before = version("mv")
    spark.sql(s"ALTER TABLE $t SET LOCATION '$other'")
    assert(path(location("mv")) == path(other))
    assert(columns("mv") == Seq("id", "other"))
    assert(version("mv") > before)
    assert(spark.sql(s"SELECT other FROM $t").count() == 0)
    if (mode == Named) assert(!spark.sessionState.catalog.tableExists(TableIdentifier("mv", Some("db"))))
  }

  test("ALTER TABLE SET LOCATION to a directory without a Delta log is refused") {
    val t = tbl("db", "mv2")
    val loc = create("mv2")
    val before = version("mv2")
    val empty = tempDir()
    val e = intercept[Exception](spark.sql(s"ALTER TABLE $t SET LOCATION '$empty'"))
    assert(fullMessage(e).contains("_delta_log"))
    assert(fullMessage(e).contains(path(empty).getPath))
    assert(version("mv2") == before)
    assert(location("mv2") == loc)
  }

  // Spark's RenameTableExec calls invalidateTable first, which loads the Delta log (syncSchema).
  test("RENAME keeps the data readable under the new name") {
    create("old")
    spark.sql(s"INSERT INTO ${tbl("db", "old")} VALUES (1, 'a')")
    spark.sql(s"ALTER TABLE ${tbl("db", "old")} RENAME TO ${tbl("db", "renamed")}")
    assert(!glue.listTableNames("db").contains("old"))
    assert(ids("renamed") == Seq(1))
  }

  test("DROP TABLE leaves the Delta log; PURGE fails and keeps the entry") {
    val loc = create("dr")
    spark.sql(s"DROP TABLE ${tbl("db", "dr")}")
    assert(!glue.listTableNames("db").contains("dr"))
    assert(hasDeltaLog(loc))
    val loc2 = create("dr2")
    intercept[Exception](spark.sql(s"DROP TABLE ${tbl("db", "dr2")} PURGE"))
    assert(glue.listTableNames("db").contains("dr2"))
    assert(hasDeltaLog(loc2))
  }

  test("path-based operations still work") {
    val loc = create("pb")
    spark.sql(s"INSERT INTO ${tbl("db", "pb")} VALUES (1, 'a')")
    spark.sql(s"MERGE INTO delta.`$loc` t USING (SELECT 2 AS id, 'b' AS name) s ON t.id = s.id WHEN NOT MATCHED THEN INSERT *")
    assert(spark.sql(s"SELECT * FROM delta.`$loc`").count() == 2)
    assert(ids("pb") == Seq(1, 2))
  }

  test("time travel on a non-Delta table says it is for Delta only") {
    spark.sql(s"CREATE TABLE ${tbl("db", "pq")} (id INT) USING parquet LOCATION '${tempDir()}'")
    val e = intercept[Exception](spark.sql(s"SELECT * FROM ${tbl("db", "pq")} VERSION AS OF 0"))
    assert(fullMessage(e).contains("Time travel is supported for Delta tables only"))
  }

  // ---- DataFrameWriter V2 ----

  private def frame[T <: Product : scala.reflect.runtime.universe.TypeTag](rows: Seq[T])(names: String*) =
    spark.createDataFrame(rows).toDF(names: _*)
  private def df(rows: (Int, String)*) = frame(rows)("id", "dt")

  test("writeTo create makes the table and its Glue entry") {
    val t = tbl("db", "w_create")
    df((1, "1"), (2, "2")).writeTo(t).using("delta").partitionedBy(col("dt")).create()
    assert(ids("w_create") == Seq(1, 2))
    assert(columns("w_create") == Seq("id", "dt"))
    assert(param("w_create", "spark.sql.sources.schema.partCol.0") == "dt")
    assert(hasDeltaLog(location("w_create")))
  }

  test("writeTo append and overwrite(condition) leave the Glue entry alone") {
    val t = tbl("db", "w_app")
    create("w_app", "id INT, dt STRING")
    val before = version("w_app")
    df((1, "1"), (2, "2")).writeTo(t).append()
    assert(ids("w_app") == Seq(1, 2))
    df((9, "2")).writeTo(t).overwrite(col("dt") === "2")
    assert(ids("w_app") == Seq(1, 9))
    assert(version("w_app") == before)
  }

  test("writeTo overwritePartitions replaces the partitions it holds") {
    val t = tbl("db", "w_part")
    val loc = tempDir()
    spark.sql(s"CREATE TABLE $t (id INT, dt STRING) USING delta PARTITIONED BY (dt) LOCATION '$loc'")
    df((1, "1"), (2, "2")).writeTo(t).append()
    val before = version("w_part")
    df((5, "2")).writeTo(t).overwritePartitions()
    assert(ids("w_part") == Seq(1, 5))
    assert(version("w_part") == before)
  }

  test("writeTo replace and createOrReplace update the Glue entry") {
    val t = tbl("db", "w_rep")
    val loc = create("w_rep", "id INT, dt STRING")
    glue.setParameter("db", "w_rep", "other_engine", "kept")
    val before = version("w_rep")
    frame(Seq((1, "a", 2.0)))("id", "name", "score").writeTo(t).using("delta").replace()
    assert(columns("w_rep") == Seq("id", "name", "score"))
    assert(version("w_rep") > before)
    assert(param("w_rep", "other_engine") == "kept")
    assert(location("w_rep") == loc)
    val mid = version("w_rep")
    frame(Seq((1, "b")))("id", "tag").writeTo(t).using("delta").createOrReplace()
    assert(columns("w_rep") == Seq("id", "tag"))
    assert(version("w_rep") > mid)
    frame(Seq((1, "b")))("id", "tag").writeTo(tbl("db", "w_cor")).using("delta").createOrReplace()
    assert(columns("w_cor") == Seq("id", "tag"))
  }

  // ---- streaming ----

  test("a stream with toTable creates the missing table, and readStream.table reads it") {
    val src = tempDir()
    val srcDir = path(src)
    Files.write(Paths.get(srcDir.getPath, "a.json"), "{\"id\":1,\"name\":\"a\"}\n{\"id\":2,\"name\":\"b\"}\n".getBytes("UTF-8"))
    val schema = new StructType().add("id", IntegerType).add("name", StringType)
    val t = tbl("db", "stream")
    val write = spark.readStream.schema(schema).json(src).writeStream.format("delta")
      .option("checkpointLocation", tempDir()).trigger(Trigger.AvailableNow()).toTable(t)
    try write.awaitTermination() finally write.stop()
    assert(glue.listTableNames("db").contains("stream"))
    assert(columns("stream") == Seq("id", "name"))
    assert(ids("stream") == Seq(1, 2))
    val readStream = spark.readStream.table(t).writeStream.format("memory").queryName(s"stream_sink_$cat")
    if (SPARK_VERSION.startsWith("3.5")) {
      // Delta 3.3 rejects the table schema that Spark 3.5 passes to its streaming source for a non-session catalog.
      val e = intercept[Exception](readStream.start().processAllAvailable())
      assert(fullMessage(e).contains("DELTA_UNSUPPORTED_SCHEMA_DURING_READ"))
    } else {
      val read = readStream.start()
      try {
        read.processAllAvailable()
        assert(spark.table(s"stream_sink_$cat").count() == 2)
      } finally read.stop()
    }
  }

  // ---- features Spark refuses or Delta accepts, recorded for the README ----

  test("ADD COLUMN with a DEFAULT and generated columns on a Delta table by name") {
    create("feat", "id INT")
    val dflt = scala.util.Try(spark.sql(s"ALTER TABLE ${tbl("db", "feat")} ADD COLUMN c INT DEFAULT 1"))
    val gen = scala.util.Try(spark.sql(
      s"CREATE TABLE ${tbl("db", "gen")} (id INT, g INT GENERATED ALWAYS AS (id + 1)) USING delta LOCATION '${tempDir()}'"))
    // Delta refuses the first with its own error; Spark refuses the second because GlueCatalog declares no
    // TableCatalogCapability.SUPPORTS_CREATE_TABLE_WITH_GENERATED_COLUMNS.
    assert(dflt.failed.get.getMessage.contains("DEFAULT values are not supported"))
    assert(gen.failed.get.getMessage.contains("does not support generated columns"))
    assert(!glue.listTableNames("db").contains("gen"))
  }
}

@RunWith(classOf[JUnitRunner])
class DeltaOperationsNamedIT extends DeltaOperationsIT(Named)

@RunWith(classOf[JUnitRunner])
class DeltaOperationsSessionIT extends DeltaOperationsIT(Session)
