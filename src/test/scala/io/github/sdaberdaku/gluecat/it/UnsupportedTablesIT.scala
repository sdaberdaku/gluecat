package io.github.sdaberdaku.gluecat.it

import scala.collection.JavaConverters._

import org.apache.spark.sql.AnalysisException
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import software.amazon.awssdk.services.glue.model.{Column, SerDeInfo, StorageDescriptor, TableInput}

/** Entries other engines write into Glue that this catalog lists or skips but never loads. */
abstract class UnsupportedTablesIT(mode: Mode) extends SparkIT(mode) {
  private def columns = Column.builder().name("id").`type`("bigint").build()

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"CREATE DATABASE $cat.db")
    glue.createRawTable("db", TableInput.builder().name("ice").tableType("EXTERNAL_TABLE")
      .parameters(Map("table_type" -> "ICEBERG", "metadata_location" -> "s3://bucket-a/ice/metadata/00000.json").asJava)
      .storageDescriptor(StorageDescriptor.builder().location("s3://bucket-a/ice").columns(columns).build())
      .build())
    glue.createRawTable("db", TableInput.builder().name("view").tableType("VIRTUAL_VIEW")
      .viewOriginalText("SELECT 1 AS id").viewExpandedText("SELECT 1 AS id")
      .storageDescriptor(StorageDescriptor.builder().columns(columns).build())
      .build())
    val parquet = "org.apache.hadoop.hive.ql.io.parquet"
    glue.createRawTable("db", TableInput.builder().name("serde").tableType("EXTERNAL_TABLE")
      .storageDescriptor(StorageDescriptor.builder().location("s3://bucket-a/hive/serde").columns(columns)
        .inputFormat(s"$parquet.MapredParquetInputFormat").outputFormat(s"$parquet.MapredParquetOutputFormat")
        .serdeInfo(SerDeInfo.builder().serializationLibrary(s"$parquet.serde.ParquetHiveSerDe").build())
        .build())
      .build())
  }

  private def selectError(t: String): String =
    intercept[AnalysisException](spark.sql(s"SELECT * FROM ${tbl("db", t)}")).getMessage

  test("SHOW TABLES lists the Iceberg and Hive SerDe entries, not the view") {
    assert(spark.sql(s"SHOW TABLES IN $cat.db").collect().map(_.getAs[String]("tableName")).toSet == Set("ice", "serde"))
  }

  test("reading an Iceberg entry fails and points to Iceberg's catalog") {
    val message = selectError("ice")
    assert(message.contains("Iceberg tables are not loaded by this catalog"))
    assert(message.contains("org.apache.iceberg.aws.glue.GlueCatalog"))
  }

  test("reading a Glue view fails") {
    assert(selectError("view").contains("Glue views are not supported"))
  }

  test("reading a Hive SerDe entry without a data source provider fails") {
    assert(selectError("serde").contains("not a Spark data source table (missing spark.sql.sources.provider)"))
  }

  // Named: Spark refuses views in a catalog that does not implement ViewCatalog.
  // Session: Spark never sends CREATE VIEW in spark_catalog to the plugin; ResolveSessionCatalog turns it into
  // CreateViewCommand on its built-in session catalog, here the in-memory one, which has no database `db`.
  test("CREATE VIEW is not supported") {
    val e = intercept[AnalysisException](spark.sql(s"CREATE VIEW $cat.db.v AS SELECT 1"))
    mode match {
      case Named => assert(e.getMessage.contains("does not support views"))
      case Session => assert(e.getMessage.contains("SCHEMA_NOT_FOUND") && e.getMessage.contains("`db`"))
    }
    assert(!glue.listTableNames("db").contains("v"))
  }
}

@RunWith(classOf[JUnitRunner])
class UnsupportedTablesNamedIT extends UnsupportedTablesIT(Named)

@RunWith(classOf[JUnitRunner])
class UnsupportedTablesSessionIT extends UnsupportedTablesIT(Session)
