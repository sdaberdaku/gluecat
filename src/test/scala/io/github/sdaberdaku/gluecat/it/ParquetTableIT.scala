package io.github.sdaberdaku.gluecat.it

import java.io.File
import java.net.URI

import scala.collection.JavaConverters._

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import software.amazon.awssdk.services.glue.model.{Column, SerDeInfo, StorageDescriptor, TableInput}

abstract class ParquetTableIT(mode: Mode) extends SparkIT(mode) {
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"CREATE DATABASE $cat.db")
  }

  private def createParquet(t: String, loc: String = tempDir()): String = {
    spark.sql(s"CREATE TABLE ${tbl("db", t)} (id INT, name STRING) USING parquet LOCATION '$loc'")
    loc
  }

  // A path write, not through the catalog.
  private def writeRows(loc: String, n: Int): Unit =
    spark.range(n).selectExpr("CAST(id AS INT) AS id", "CAST(id AS STRING) AS name").write.mode("overwrite").parquet(loc)

  private def count(t: String): Long = spark.sql(s"SELECT count(*) FROM ${tbl("db", t)}").head().getLong(0)
  private def tableNames(db: String): Seq[String] =
    spark.sql(s"SHOW TABLES IN $cat.$db").collect().map(_.getAs[String]("tableName")).toSeq

  test("CREATE TABLE USING parquet LOCATION reads the files at that path") {
    val loc = createParquet("p_read")
    writeRows(loc, 3)
    assert(count("p_read") == 3)
    val entry = glue.getTable("db", "p_read")
    assert(entry.storageDescriptor().serdeInfo().serializationLibrary() ==
      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
    assert(entry.parameters().get("spark.sql.sources.provider") == "parquet")
  }

  // The plan expected this to fail. Spark (3.5.9 and 4.1.3, RelationResolution.createRelation) resolves a V1Table
  // whose partitions are not tracked in a catalog through its V1 file source path in both modes, so INSERT writes
  // files at the table location; the catalog is not called and the Glue entry stays as it was.
  test("INSERT INTO and INSERT OVERWRITE a parquet table write files at its location and leave the Glue entry alone") {
    createParquet("p_insert")
    val before = glue.getTable("db", "p_insert").versionId()
    spark.sql(s"INSERT INTO ${tbl("db", "p_insert")} VALUES (1, 'a')")
    assert(count("p_insert") == 1)
    spark.sql(s"INSERT OVERWRITE ${tbl("db", "p_insert")} VALUES (2, 'b'), (3, 'c')")
    assert(count("p_insert") == 2)
    assert(glue.getTable("db", "p_insert").versionId() == before)
  }

  test("CREATE TABLE USING parquet AS SELECT fails and points to Delta") {
    val e = intercept[Exception](spark.sql(s"CREATE TABLE ${tbl("db", "p_ctas")} USING parquet AS SELECT 1 AS id"))
    assert(fullMessage(e).contains("Delta"))
    assert(!glue.listTableNames("db").contains("p_ctas"))
  }

  test("DESCRIBE TABLE shows the columns") {
    createParquet("p_describe")
    val columns = spark.sql(s"DESCRIBE TABLE ${tbl("db", "p_describe")}").collect().map(r => r.getString(0) -> r.getString(1))
    assert(columns.take(2).toSeq == Seq("id" -> "int", "name" -> "string"))
  }

  private def described(t: String): Seq[String] =
    spark.sql(s"DESCRIBE TABLE ${tbl("db", t)}").collect().map(_.getString(0))
      .takeWhile(c => c.nonEmpty && !c.startsWith("#")).toSeq
  private def selected(t: String): Seq[String] = spark.table(tbl("db", t)).columns.toSeq

  // Spark's V1 relation is the data columns followed by the partition columns, so the catalog returns that order, as
  // HiveExternalCatalog does; otherwise DESCRIBE and positional INSERTs disagree with SELECT *.
  test("partition columns come last in DESCRIBE and SELECT *, and a positional INSERT fills the right columns") {
    spark.sql(s"CREATE TABLE ${tbl("db", "p_order")} (p STRING, id INT) USING parquet PARTITIONED BY (p) " +
      s"LOCATION '${tempDir()}'")
    assert(described("p_order") == Seq("id", "p"))
    assert(selected("p_order") == Seq("id", "p"))
    spark.sql(s"INSERT INTO ${tbl("db", "p_order")} VALUES (1, 'a')")
    val row = spark.sql(s"SELECT id, p FROM ${tbl("db", "p_order")}").head()
    assert(row.getInt(0) == 1 && row.getString(1) == "a")
  }

  test("ADD COLUMNS on a partitioned parquet table keeps the partition column last") {
    spark.sql(s"CREATE TABLE ${tbl("db", "p_add")} (id INT, p STRING) USING parquet PARTITIONED BY (p) " +
      s"LOCATION '${tempDir()}'")
    spark.sql(s"ALTER TABLE ${tbl("db", "p_add")} ADD COLUMNS (x STRING)")
    assert(described("p_add") == Seq("id", "x", "p"))
    assert(selected("p_add") == Seq("id", "x", "p"))
  }

  test("SHOW TABLES lists the table") {
    createParquet("p_show")
    assert(tableNames("db").contains("p_show"))
  }

  test("ALTER TABLE SET TBLPROPERTIES writes a Glue parameter") {
    createParquet("p_props")
    spark.sql(s"ALTER TABLE ${tbl("db", "p_props")} SET TBLPROPERTIES ('k'='v')")
    assert(glue.getTable("db", "p_props").parameters().get("k") == "v")
  }

  test("RENAME keeps the data readable and DROP keeps the files") {
    val loc = createParquet("p_old")
    writeRows(loc, 2)
    spark.sql(s"ALTER TABLE ${tbl("db", "p_old")} RENAME TO ${tbl("db", "p_new")}")
    assert(glue.listTableNames("db").contains("p_new"))
    assert(!glue.listTableNames("db").contains("p_old"))
    assert(count("p_new") == 2)
    spark.sql(s"DROP TABLE ${tbl("db", "p_new")}")
    assert(!glue.listTableNames("db").contains("p_new"))
    assert(new File(new URI(loc)).listFiles().exists(_.getName.endsWith(".parquet")))
  }

  test("DROP TABLE PURGE fails") {
    createParquet("p_purge")
    val e = intercept[Exception](spark.sql(s"DROP TABLE ${tbl("db", "p_purge")} PURGE"))
    assert(fullMessage(e).contains("PURGE"))
    assert(glue.listTableNames("db").contains("p_purge"))
  }

  test("REFRESH TABLE on a parquet table is a no-op") {
    createParquet("p_refresh")
    val before = glue.getTable("db", "p_refresh").versionId()
    spark.sql(s"REFRESH TABLE ${tbl("db", "p_refresh")}")
    assert(glue.getTable("db", "p_refresh").versionId() == before)
  }

  test("mixed-case names resolve to the lowercase Glue entries") {
    spark.sql(s"CREATE DATABASE $cat.MyDb")
    spark.sql(s"CREATE TABLE $cat.MyDb.MyTable (id INT) USING parquet LOCATION '${tempDir()}'")
    assert(glue.listTableNames("mydb") == Seq("mytable"))
    assert(tableNames("mydb") == Seq("mytable"))
    assert(spark.sql(s"SELECT * FROM $cat.MYDB.MYTABLE").count() == 0)
  }

  // Named: Spark refuses a V1Table whose partitions live in a non-session catalog (no batch read support).
  // Session: Spark reads it through its V1 path, which lists the partitions in the in-memory catalog, which has no
  // database `db`, so it fails too.
  test("a parquet table with partitions tracked in the catalog cannot be read") {
    val loc = tempDir()
    val schema = StructType(Seq(StructField("id", IntegerType), StructField("p", StringType)))
    val parquet = "org.apache.hadoop.hive.ql.io.parquet"
    glue.createRawTable("db", TableInput.builder().name("p_tracked").tableType("EXTERNAL_TABLE")
      .parameters(Map(
        "EXTERNAL" -> "TRUE",
        "spark.sql.sources.provider" -> "parquet",
        "spark.sql.partitionProvider" -> "catalog",
        "spark.sql.sources.schema" -> schema.json,
        "spark.sql.sources.schema.numPartCols" -> "1",
        "spark.sql.sources.schema.partCol.0" -> "p").asJava)
      .partitionKeys(Column.builder().name("p").`type`("string").build())
      .storageDescriptor(StorageDescriptor.builder().location(loc)
        .columns(Column.builder().name("id").`type`("int").build())
        .inputFormat(s"$parquet.MapredParquetInputFormat")
        .outputFormat(s"$parquet.MapredParquetOutputFormat")
        .serdeInfo(SerDeInfo.builder().serializationLibrary(s"$parquet.serde.ParquetHiveSerDe")
          .parameters(Map("path" -> loc, "serialization.format" -> "1").asJava).build())
        .build())
      .build())
    intercept[Exception](spark.sql(s"SELECT * FROM ${tbl("db", "p_tracked")}").collect())
  }
}

@RunWith(classOf[JUnitRunner])
class ParquetTableNamedIT extends ParquetTableIT(Named)

@RunWith(classOf[JUnitRunner])
class ParquetTableSessionIT extends ParquetTableIT(Session)
