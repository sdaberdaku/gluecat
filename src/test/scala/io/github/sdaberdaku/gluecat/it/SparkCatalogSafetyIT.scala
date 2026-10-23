package io.github.sdaberdaku.gluecat.it

import java.io.File
import java.net.URI

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import software.amazon.awssdk.services.glue.model.{StorageDescriptor, TableInput}

import io.github.sdaberdaku.gluecat.it.IsolationIT.HookConf

/** Delta writes by name into the catalog under test (account 111) leave alone a same-named `db.t` in account 999 and
  * one in Spark's v1 session catalog, which is where Delta's UpdateCatalog hook and its SessionCatalog fallbacks look,
  * and so do non-Delta writes, where Spark's statistics auto-update looks (`db.p`).
  * Account 999 is `spark_catalog` in Named mode and the named catalog `acct_other` in Session mode. */
abstract class SparkCatalogSafetyIT(mode: Mode) extends SparkIT(mode) {
  private val other = if (mode == Named) "spark_catalog" else "acct_other"
  override protected def otherCatalogs: Map[String, Map[String, String]] = Map(other -> IsolationIT.Account999)

  // Through the session catalog's own resolution in Session mode.
  private val target = if (mode == Named) tbl("db", "t") else "db.t"
  private val parquetTarget = if (mode == Named) tbl("db", "p") else "db.p"
  private val v1Table = TableIdentifier("t", Some("db"))
  private val v1Parquet = TableIdentifier("p", Some("db"))
  private val StatsConf = "spark.sql.statistics.size.autoUpdate.enabled"
  private var parquetLocation: String = _
  private val glue999 = new GlueInspector("999999999999")
  private var version999: String = _
  private var v1Before: CatalogTable = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    glue999.createDatabase("db")
    glue999.createRawTable("db", TableInput.builder().name("t").tableType("EXTERNAL_TABLE")
      .parameters(Map("spark.sql.sources.provider" -> "delta", "table_type" -> "DELTA", "EXTERNAL" -> "TRUE").asJava)
      .storageDescriptor(StorageDescriptor.builder().location(tempDir()).build()).build())
    version999 = glue999.getTable("db", "t").versionId()
    assert(version999 != null)

    val location = tempDir()
    val v1 = spark.sessionState.catalog
    v1.createDatabase(CatalogDatabase("db", "", new URI(tempDir()), Map.empty), ignoreIfExists = false)
    v1.createTable(CatalogTable(identifier = v1Table, tableType = CatalogTableType.EXTERNAL,
      storage = CatalogStorageFormat.empty.copy(locationUri = Some(new URI(location))),
      schema = new StructType().add("v1_only", StringType), provider = Some("delta")), ignoreIfExists = false)
    v1Before = v1.getTableMetadata(v1Table)
    v1.createTable(CatalogTable(identifier = v1Parquet, tableType = CatalogTableType.EXTERNAL,
      storage = CatalogStorageFormat.empty.copy(locationUri = Some(new URI(tempDir()))),
      schema = new StructType().add("id", IntegerType), provider = Some("parquet")), ignoreIfExists = false)

    spark.sql(s"CREATE DATABASE $cat.db")
    spark.sql(s"CREATE TABLE ${tbl("db", "t")} (id INT) USING delta LOCATION '$location'")
    parquetLocation = tempDir()
    spark.sql(s"CREATE TABLE ${tbl("db", "p")} (id INT) USING parquet LOCATION '$parquetLocation'")
  }

  override def afterAll(): Unit = try glue999.close() finally super.afterAll()

  private def othersUntouched(): Unit = {
    assert(glue999.getTable("db", "t").versionId() == version999)
    val v1 = spark.sessionState.catalog.getTableMetadata(v1Table)
    assert(v1.schema == v1Before.schema)
    assert(v1.properties == v1Before.properties)
    assert(v1.storage == v1Before.storage)
    assert(v1.stats == v1Before.stats)
    assert(spark.sessionState.catalog.getTableMetadata(v1Parquet).stats.isEmpty)
  }

  private def rows(): Long = spark.table(tbl("db", "t")).count()

  test("with the hook off, writes by name touch neither the other account nor the v1 session catalog") {
    spark.sql(s"SHOW NAMESPACES IN $other").collect()
    val before = rows()
    spark.sql(s"INSERT INTO $target VALUES (1)")
    assert(rows() == before + 1)
    othersUntouched()

    val recorded = TestGlueCatalog.recorded.asScala.toList
    assert(recorded.exists(_._1 == other))
    val writes = recorded.filter(r => IsolationIT.Writes(r._2))
    assert(writes.nonEmpty)
    assert(writes.forall(w => w._1 == cat && w._3.contains("111111111111")), writes)
  }

  test("the hook turned on at runtime is refused before Delta runs") {
    val before = rows()
    try {
      spark.conf.set(HookConf, "true")
      Seq(s"INSERT INTO $target VALUES (2)", s"SELECT * FROM $target").foreach { q =>
        assert(fullMessage(intercept[Exception](spark.sql(q).collect())).contains("catalog.update.enabled"), q)
      }
      othersUntouched()
      spark.conf.set(HookConf, "false")
      spark.sql(s"INSERT INTO $target VALUES (3)")
      assert(rows() == before + 1)
      othersUntouched()
    } finally spark.conf.unset(HookConf)
  }

  // Spark's InsertIntoHadoopFsRelationCommand would read and alter the statistics of `db.p` in the v1 session catalog
  // (or fail after writing when there is none), so non-Delta loads and creates by name are refused while it is on.
  test("statistics auto-update turned on is refused for non-Delta tables before anything is written") {
    try {
      spark.conf.set(StatsConf, "true")
      Seq(s"INSERT INTO $parquetTarget VALUES (1)", s"SELECT * FROM $parquetTarget",
        s"CREATE TABLE ${tbl("db", "p2")} (id INT) USING parquet LOCATION '${tempDir()}'").foreach { q =>
        assert(fullMessage(intercept[Exception](spark.sql(q).collect())).contains(StatsConf), q)
      }
      assert(!glue.listTableNames("db").contains("p2"))
      assert(new File(new URI(parquetLocation)).list().isEmpty)
      othersUntouched()
      // Delta writes do not go through that path.
      val before = rows()
      spark.sql(s"INSERT INTO $target VALUES (4)")
      assert(rows() == before + 1)
      othersUntouched()
    } finally spark.conf.unset(StatsConf)
    spark.sql(s"INSERT INTO $parquetTarget VALUES (1)")
    assert(spark.table(tbl("db", "p")).count() == 1)
    othersUntouched()
  }
}

@RunWith(classOf[JUnitRunner])
class SparkCatalogSafetyNamedIT extends SparkCatalogSafetyIT(Named)

@RunWith(classOf[JUnitRunner])
class SparkCatalogSafetySessionIT extends SparkCatalogSafetyIT(Session)
