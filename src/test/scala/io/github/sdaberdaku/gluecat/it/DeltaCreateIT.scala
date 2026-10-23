package io.github.sdaberdaku.gluecat.it

import java.io.File
import java.net.URI

import scala.collection.JavaConverters._

import io.delta.tables.DeltaTable
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import io.github.sdaberdaku.gluecat.catalog.DeltaCommittedButGlueFailedException
import io.github.sdaberdaku.gluecat.glue.GlueFailure

/** The gate: Delta creates run by path, then the Glue entry is written; nothing goes through spark_catalog. */
abstract class DeltaCreateIT(mode: Mode) extends SparkIT(mode) {
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"CREATE DATABASE $cat.db")
  }

  private def hasDeltaLog(loc: String): Boolean = new File(new File(new URI(loc).getPath), "_delta_log").isDirectory
  private def columns(t: String): Seq[String] =
    glue.getTable("db", t).storageDescriptor().columns().asScala.map(_.name()).toList
  private def rows(sql: String): Seq[(Int, String)] =
    spark.sql(sql).collect().map(r => (r.getInt(0), r.getString(1))).toSeq
  private def causalChain(e: Throwable): Seq[Throwable] = Iterator.iterate(e)(_.getCause).takeWhile(_ != null).toList
  // Path identifiers (delta.`<path>`) must never become Glue table requests. The only request naming `delta` comes from
  // Spark 4.1's ApplyDefaultCollationToStringType, which reads the namespace's default collation for CREATE TABLE
  // delta.`<path>` (GetDatabase) and ignores any error from it.
  private def noPathLookups(): Unit =
    assert(TestGlueCatalog.requests.asScala.filter(r => r._3.contains("delta") && r._2 != "GetDatabase").isEmpty)

  test("CREATE TABLE USING delta LOCATION registers the table and loads it as Delta") {
    val loc = tempDir()
    spark.sql(s"CREATE TABLE ${tbl("db", "t1")} (id INT, name STRING) USING delta LOCATION '$loc'")
    assert(hasDeltaLog(loc))
    val entry = glue.getTable("db", "t1")
    assert(entry.parameters().get("spark.sql.sources.provider") == "delta")
    assert(entry.parameters().get("table_type") == "DELTA")
    assert(entry.parameters().get("EXTERNAL") == "TRUE")
    assert(entry.storageDescriptor().location() == loc)
    assert(entry.storageDescriptor().serdeInfo().parameters().get("path") == loc)
    assert(columns("t1") == Seq("id", "name"))
    assert(entry.partitionKeys().isEmpty)
    assert(spark.sql(s"DESCRIBE DETAIL ${tbl("db", "t1")}").select("format").head.getString(0) == "delta")
  }

  // Spark passes no provider for CREATE TABLE without USING; the catalog takes spark.sql.sources.default.
  test("CREATE TABLE without USING follows spark.sql.sources.default") {
    try {
      for (provider <- Seq("json", "delta")) {
        spark.conf.set("spark.sql.sources.default", provider)
        val loc = tempDir()
        spark.sql(s"CREATE TABLE ${tbl("db", s"d_$provider")} (id INT) LOCATION '$loc'")
        assert(glue.getTable("db", s"d_$provider").parameters().get("spark.sql.sources.provider") == provider)
        assert(hasDeltaLog(loc) == (provider == "delta"))
      }
    } finally spark.conf.unset("spark.sql.sources.default")
  }

  test("CTAS by name") {
    val loc = tempDir()
    spark.sql(s"CREATE TABLE ${tbl("db", "t2")} USING delta LOCATION '$loc' AS SELECT 1 AS id, 'a' AS name")
    assert(spark.sql(s"SELECT * FROM ${tbl("db", "t2")}").count() == 1)
    assert(glue.getTable("db", "t2").parameters().get("spark.sql.sources.provider") == "delta")
    assert(columns("t2") == Seq("id", "name"))
    assert(hasDeltaLog(loc))
    noPathLookups()
  }

  test("saveAsTable with the path option, kafka2delta style") {
    val loc = tempDir()
    val empty = spark.createDataFrame(Seq.empty[(Int, String)]).toDF("id", "name")
    empty.write.format("delta").option("path", loc).saveAsTable(tbl("db", "t3"))
    assert(spark.catalog.tableExists(tbl("db", "t3")))
    assert(glue.getTable("db", "t3").storageDescriptor().location() == loc)
    assert(columns("t3") == Seq("id", "name"))
    intercept[TableAlreadyExistsException] {
      empty.write.format("delta").option("path", loc).saveAsTable(tbl("db", "t3"))
    }
    noPathLookups()
  }

  test("saveAsTable with a path that already holds a Delta table fails") {
    val loc = tempDir()
    spark.range(1).write.format("delta").save(loc)
    intercept[Exception] {
      spark.range(2).write.format("delta").option("path", loc).saveAsTable(tbl("db", "t4"))
    }
    assert(!glue.listTableNames("db").contains("t4"))
    assert(spark.read.format("delta").load(loc).count() == 1)
    noPathLookups()
  }

  test("INSERT and MERGE by name, MERGE by path, read back") {
    val loc = tempDir()
    val t = tbl("db", "t5")
    spark.sql(s"CREATE TABLE $t (id INT, name STRING) USING delta LOCATION '$loc'")
    spark.sql(s"INSERT INTO $t VALUES (1, 'a')")
    spark.sql(s"MERGE INTO $t t USING (SELECT 1 AS id, 'b' AS name) s ON t.id = s.id " +
      "WHEN MATCHED THEN UPDATE SET name = s.name")
    spark.sql(s"MERGE INTO delta.`$loc` t USING (SELECT 2 AS id, 'c' AS name) s ON t.id = s.id " +
      "WHEN NOT MATCHED THEN INSERT *")
    assert(rows(s"SELECT id, name FROM $t ORDER BY id") == Seq((1, "b"), (2, "c")))
    noPathLookups()
  }

  test("partitioned CTAS writes partition columns in the Glue schema, not as partition keys") {
    val loc = tempDir()
    spark.sql(s"CREATE TABLE ${tbl("db", "t6")} USING delta PARTITIONED BY (p) LOCATION '$loc' " +
      "AS SELECT 1 AS id, 'x' AS p")
    val entry = glue.getTable("db", "t6")
    assert(columns("t6") == Seq("id", "p"))
    assert(entry.partitionKeys().isEmpty)
    assert(entry.parameters().get("spark.sql.sources.schema.numPartCols") == "1")
    assert(entry.parameters().get("spark.sql.sources.schema.partCol.0") == "p")
    val detail = spark.sql(s"DESCRIBE DETAIL ${tbl("db", "t6")}").select("partitionColumns").head.getSeq[String](0)
    assert(detail == Seq("p"))
    noPathLookups()
  }

  if (mode == Named) test("nothing is registered in spark_catalog") {
    val loc = tempDir()
    spark.sql(s"CREATE TABLE ${tbl("db", "t7")} USING delta LOCATION '$loc' AS SELECT 1 AS id")
    spark.range(1).write.format("delta").option("path", tempDir()).saveAsTable(tbl("db", "t8"))
    val session = spark.sessionState.catalog
    assert(!session.tableExists(TableIdentifier("t7", Some("db"))))
    assert(!session.tableExists(TableIdentifier("t8", Some("db"))))
    assert(session.listDatabases() == Seq("default"))
    assert(session.listTables("default").isEmpty)
  }

  test("Glue failure after the Delta commit") {
    val loc = tempDir()
    val t = tbl("db", "t9")
    TestGlueCatalog.failNext(cat, "CreateTable", new GlueFailure("CreateTable", "injected"))
    val e = intercept[Exception](spark.sql(s"CREATE TABLE $t USING delta LOCATION '$loc' AS SELECT 1 AS id"))
    assert(causalChain(e).exists(_.isInstanceOf[DeltaCommittedButGlueFailedException]))
    assert(e.getMessage.contains(s"CREATE TABLE $t USING delta LOCATION '$loc'"))
    assert(hasDeltaLog(loc))
    assert(!glue.listTableNames("db").contains("t9"))
    spark.sql(s"CREATE TABLE $t USING delta LOCATION '$loc'") // the recovery registers the existing table
    assert(spark.sql(s"SELECT * FROM $t").count() == 1)
    assert(columns("t9") == Seq("id"))
    noPathLookups()
  }

  // As spark_catalog these reach gluecat; Named mode sends them to Delta's own spark_catalog.
  test("Delta path identifiers go to Delta alone") {
    val loc = tempDir()
    spark.sql(s"CREATE TABLE delta.`$loc` (id INT) USING delta")
    assert(hasDeltaLog(loc))
    spark.sql(s"ALTER TABLE delta.`$loc` ADD COLUMNS (x STRING)")
    assert(spark.read.format("delta").load(loc).columns.toSeq == Seq("id", "x"))
    val loc2 = tempDir()
    DeltaTable.createOrReplace(spark).location(loc2).addColumn("id", "INT").execute()
    assert(hasDeltaLog(loc2))
    assert(!glue.listDatabaseNames().contains("delta"))
    noPathLookups()
  }

  test("Delta creates need spark.sql.runSQLOnFiles") {
    val loc = tempDir()
    spark.conf.set("spark.sql.runSQLOnFiles", "false")
    try {
      val e = intercept[Exception](spark.sql(s"CREATE TABLE ${tbl("db", "t13")} (id INT) USING delta LOCATION '$loc'"))
      assert(fullMessage(e).contains("gluecat: Delta tables need spark.sql.runSQLOnFiles=true (default); it is false"))
    } finally spark.conf.unset("spark.sql.runSQLOnFiles")
    assert(!hasDeltaLog(loc))
    assert(!glue.listTableNames("db").contains("t13"))
  }

  // Called on the catalog directly: Spark would qualify (or reject) the location before it reaches the catalog.
  test("REPLACE refuses a LOCATION on another authority and accepts a trailing slash") {
    val loc = tempDir()
    spark.sql(s"CREATE TABLE ${tbl("db", "t12")} (id INT) USING delta LOCATION '$loc'")
    val catalog = new TestGlueCatalog()
    catalog.initialize("direct", new CaseInsensitiveStringMap(Map("glue.id" -> "111111111111",
      "client.region" -> "eu-central-1", "glue.endpoint" -> MiniStack.Endpoint).asJava))
    def replace(location: String) = catalog.stageReplace(Identifier.of(Array("db"), "t12"),
      new StructType().add("id", IntegerType), Array.empty[Transform], Map("provider" -> "delta", "location" -> location).asJava)
    val e = intercept[IllegalArgumentException](replace(loc.replace("file:/", "file://host-b/")))
    assert(e.getMessage.contains("REPLACE cannot move"))
    replace(loc + "/").abortStagedChanges()
    assert(glue.getTable("db", "t12").storageDescriptor().location() == loc)
  }

  // Task 12 has the full re-sync tests; these two prove the plumbing.
  test("REFRESH TABLE re-syncs the Glue columns after a path-only schema change") {
    val loc = tempDir()
    val t = tbl("db", "t10")
    spark.sql(s"CREATE TABLE $t (id INT) USING delta LOCATION '$loc'")
    spark.sql("SELECT 1 AS id, 'e' AS extra").write.format("delta").option("mergeSchema", "true").mode("append").save(loc)
    assert(columns("t10") == Seq("id"))
    spark.sql(s"REFRESH TABLE $t")
    assert(columns("t10") == Seq("id", "extra"))
    assert(glue.getTable("db", "t10").parameters().get("spark.sql.sources.schema").contains("extra"))
    noPathLookups()
  }

  test("ALTER TABLE ADD COLUMNS re-syncs the Glue columns") {
    val loc = tempDir()
    val t = tbl("db", "t11")
    spark.sql(s"CREATE TABLE $t (id INT) USING delta LOCATION '$loc'")
    spark.sql(s"ALTER TABLE $t ADD COLUMNS (extra STRING)")
    assert(columns("t11") == Seq("id", "extra"))
    assert(spark.sql(s"SELECT extra FROM $t").count() == 0)
  }
}

@RunWith(classOf[JUnitRunner])
class DeltaCreateNamedIT extends DeltaCreateIT(Named)

@RunWith(classOf[JUnitRunner])
class DeltaCreateSessionIT extends DeltaCreateIT(Session)
