package io.github.sdaberdaku.gluecat.it

import scala.collection.JavaConverters._

import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import software.amazon.awssdk.services.glue.model.{StorageDescriptor, TableInput}

/** A Delta write by name into the catalog under test (account 111) leaves a same-named table in account 999 alone.
  * Account 999 is `spark_catalog` in Named mode and the named catalog `acct_other` in Session mode. */
abstract class SparkCatalogSafetyIT(mode: Mode) extends SparkIT(mode) {
  private val other = if (mode == Named) "spark_catalog" else "acct_other"
  override protected def otherCatalogs: Map[String, Map[String, String]] = Map(other -> IsolationIT.Account999)

  private val glue999 = new GlueInspector("999999999999")
  override def afterAll(): Unit = try glue999.close() finally super.afterAll()

  test("with the hook off, writes by name do not touch the same db.table in another account") {
    glue999.createDatabase("db")
    glue999.createRawTable("db", TableInput.builder().name("t").tableType("EXTERNAL_TABLE")
      .parameters(Map("spark.sql.sources.provider" -> "delta", "table_type" -> "DELTA", "EXTERNAL" -> "TRUE").asJava)
      .storageDescriptor(StorageDescriptor.builder().location(tempDir()).build()).build())
    val versionBefore = glue999.getTable("db", "t").versionId()
    assert(versionBefore != null)

    spark.sql(s"CREATE DATABASE $cat.db")
    spark.sql(s"CREATE TABLE ${tbl("db", "t")} (id INT) USING delta LOCATION '${tempDir()}'")
    spark.sql(s"SHOW NAMESPACES IN $other").collect()
    // Through the session catalog's own resolution in Session mode.
    spark.sql(s"INSERT INTO ${if (mode == Named) tbl("db", "t") else "db.t"} VALUES (1)")
    assert(spark.table(tbl("db", "t")).count() == 1)

    assert(glue999.getTable("db", "t").versionId() == versionBefore)
    val recorded = TestGlueCatalog.recorded.asScala.toList
    assert(recorded.exists(_._1 == other))
    val writes = recorded.filter(r => IsolationIT.Writes(r._2))
    assert(writes.nonEmpty)
    assert(writes.forall(w => w._1 == cat && w._3.contains("111111111111")), writes)
  }
}

@RunWith(classOf[JUnitRunner])
class SparkCatalogSafetyNamedIT extends SparkCatalogSafetyIT(Named)

@RunWith(classOf[JUnitRunner])
class SparkCatalogSafetySessionIT extends SparkCatalogSafetyIT(Session)
