package io.github.sdaberdaku.gluecat.catalog

import java.net.URI

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.types.{DataType, StructType}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EntryConverterReadSuite extends AnyFunSuite {
  private def convert(name: String) = EntryConverter.toCatalogTable("acct_a", Fixtures.table(name))

  test("delta-spark35-ctas: empty schema, path from the SerDe, hidden parameters removed") {
    val t = convert("delta-spark35-ctas")
    assert(t.provider.contains("delta"))
    assert(t.storage.locationUri.contains(new URI("s3a://bucket-a/delta/t")))
    assert(t.schema.isEmpty)
    assert(t.tracksPartitionsInCatalog)
    assert(t.tableType == CatalogTableType.EXTERNAL)
    assert(t.identifier == TableIdentifier("t", Some("db"), Some("acct_a")))
    assert(!t.properties.contains("EXTERNAL") && !t.properties.contains("transient_lastDdlTime"))
    assert(!t.properties.keys.exists(_.startsWith("spark.sql.")))
    assert(t.createVersion == "3.5.5")
  }

  test("delta-rewritten: real schema and Delta properties kept") {
    val t = convert("delta-rewritten")
    assert(t.schema.fieldNames.toSeq == Seq("id", "amount", "ts"))
    assert(t.properties("delta.lastUpdateVersion") == "2880")
    assert(t.properties("numFiles") == "0")
    assert(!t.tracksPartitionsInCatalog)
  }

  test("delta-by-this-catalog: table_type hidden, real location") {
    val t = convert("delta-by-this-catalog")
    assert(!t.properties.contains("table_type"))
    assert(t.storage.locationUri.contains(new URI("s3://bucket-a/delta/t")))
  }

  test("parquet-hive-compatible: partition columns, schema and storage formats") {
    val t = convert("parquet-hive-compatible")
    assert(t.partitionColumnNames == Seq("dt"))
    assert(t.schema.fieldNames.toSeq == Seq("id", "name", "dt"))
    assert(t.storage.inputFormat.contains("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"))
    assert(!t.storage.properties.contains("path") && !t.storage.properties.contains("serialization.format"))
  }

  private def withSchema(name: String, schema: StructType, partCol: String) = {
    val e = Fixtures.table(name)
    e.copy(parameters = e.parameters ++ Map("spark.sql.sources.schema" -> schema.json,
      "spark.sql.sources.schema.numPartCols" -> "1", "spark.sql.sources.schema.partCol.0" -> partCol))
  }
  private val dtFirst = new StructType().add("dt", "string").add("id", "long").add("name", "string")

  test("non-Delta: partition columns are moved to the end, in partCol order") {
    val t = EntryConverter.toCatalogTable("acct_a", withSchema("parquet-hive-compatible", dtFirst, "dt"))
    assert(t.schema.fieldNames.toSeq == Seq("id", "name", "dt"))
    assert(t.partitionSchema.fieldNames.toSeq == Seq("dt"))
  }

  test("non-Delta: a partition column missing from the schema is a malformed entry naming table and column") {
    val e = intercept[MalformedEntryException](
      EntryConverter.toCatalogTable("acct_a", withSchema("parquet-hive-compatible", dtFirst, "region")))
    assert(e.getMessage.contains("acct_a.db.t") && e.getMessage.contains("region"))
  }

  test("Delta: the stored column order is kept") {
    val t = EntryConverter.toCatalogTable("acct_a", withSchema("delta-rewritten", dtFirst, "dt"))
    assert(t.schema.fieldNames.toSeq == Seq("dt", "id", "name"))
  }

  test("json-spark-specific: location from path, schema from the parameter") {
    val t = convert("json-spark-specific")
    assert(t.storage.locationUri.contains(new URI("s3://bucket-a/json/t")))
    assert(t.schema.fieldNames.toSeq == Seq("a"))
  }

  test("schema-in-parts: parts are joined in order") {
    val e = Fixtures.table("schema-in-parts")
    val joined = (0 until 2).map(i => e.parameters(s"spark.sql.sources.schema.part.$i")).mkString
    assert(EntryConverter.toCatalogTable("acct_a", e).schema == DataType.fromJson(joined).asInstanceOf[StructType])
  }

  test("iceberg is rejected with a pointer to the Iceberg catalog") {
    val e = intercept[UnsupportedTableException](convert("iceberg"))
    assert(e.getMessage.contains("Iceberg") && e.getMessage.contains("org.apache.iceberg.spark.SparkCatalog") &&
      e.getMessage.contains("GlueCatalog"))
  }

  test("uppercase table_type still detected") {
    val e = Fixtures.table("iceberg")
    val lower = e.copy(parameters = e.parameters.updated("table_type", "iceberg"))
    intercept[UnsupportedTableException](EntryConverter.toCatalogTable("acct_a", lower))
  }

  test("views are rejected") {
    assert(intercept[UnsupportedTableException](convert("view")).getMessage.contains("view"))
  }

  test("entries without a data source provider are rejected") {
    val e = intercept[UnsupportedTableException](convert("hive-serde"))
    assert(e.getMessage.contains("spark.sql.sources.provider"))
  }

  test("malformed schema JSON") {
    val e = intercept[MalformedEntryException](convert("malformed-schema"))
    assert(e.getMessage.contains("acct_a.db.t") && e.getMessage.contains("schema"))
  }

  test("missing schema part") {
    assert(intercept[MalformedEntryException](convert("missing-part")).getMessage.contains("part.1"))
  }

  test("missing location") {
    assert(intercept[MalformedEntryException](convert("missing-location")).getMessage.contains("location"))
  }

  test("locations with characters that are illegal in a URI load like Spark does") {
    val e = Fixtures.table("delta-by-this-catalog")
    val spaced = e.copy(storage = e.storage.copy(serdeParameters = Map("path" -> "s3://bucket-a/my dir/t")))
    val t = EntryConverter.toCatalogTable("acct_a", spaced)
    assert(t.storage.locationUri.map(_.getPath).exists(_.endsWith("/my dir/t")))
  }
}
