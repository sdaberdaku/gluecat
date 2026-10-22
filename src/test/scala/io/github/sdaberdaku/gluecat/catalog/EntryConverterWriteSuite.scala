package io.github.sdaberdaku.gluecat.catalog

import scala.collection.JavaConverters._

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import io.github.sdaberdaku.gluecat.glue.{GlueColumn, GlueDatabaseEntry}

@RunWith(classOf[JUnitRunner])
class EntryConverterWriteSuite extends AnyFunSuite {
  private def ct(provider: String, schema: StructType, partitionCols: Seq[String], location: String,
      props: Map[String, String] = Map.empty, comment: Option[String] = None, owner: String = ""): CatalogTable =
    CatalogTable(
      identifier = TableIdentifier("t", Some("db"), Some("acct_a")),
      tableType = CatalogTableType.EXTERNAL,
      storage = CatalogStorageFormat.empty.copy(locationUri = Some(CatalogUtils.stringToURI(location))),
      schema = schema, provider = Some(provider), partitionColumnNames = partitionCols,
      comment = comment, owner = owner, createVersion = SPARK_VERSION, properties = props)

  private val idDt = new StructType().add("id", "string").add("dt", "string")
  private val idNameDt = new StructType().add("id", "bigint").add("name", "string").add("dt", "string")
  private val loc = "s3://bucket-a/d/t"

  test("delta form") {
    val e = EntryConverter.toGlueTable(ct("delta", idDt, Seq("dt"), loc, Map("delta.appendOnly" -> "true")), 4000)
    assert(e.tableType.contains("EXTERNAL_TABLE"))
    val expected = Map(
      "EXTERNAL" -> "TRUE", "table_type" -> "DELTA", "spark.sql.sources.provider" -> "delta",
      "spark.sql.create.version" -> SPARK_VERSION, "spark.sql.partitionProvider" -> "filesystem",
      "spark.sql.sources.schema" -> idDt.json, "spark.sql.sources.schema.numPartCols" -> "1",
      "spark.sql.sources.schema.partCol.0" -> "dt", "delta.appendOnly" -> "true")
    expected.foreach { case (k, v) => assert(e.parameters.get(k).contains(v), k) }
    assert(e.storage.location.contains(loc))
    assert(e.storage.serdeParameters == Map("serialization.format" -> "1", "path" -> loc))
    assert(e.storage.serdeLibrary.contains("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
    assert(e.storage.inputFormat.contains("org.apache.hadoop.mapred.SequenceFileInputFormat"))
    assert(e.storage.outputFormat.contains("org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat"))
    assert(e.storage.columns == Seq(GlueColumn("id", "string"), GlueColumn("dt", "string")))
    assert(e.partitionKeys.isEmpty)
  }

  test("parquet hive-compatible form") {
    val e = EntryConverter.toGlueTable(ct("parquet", idNameDt, Seq("dt"), loc), 4000)
    assert(e.storage.serdeLibrary.contains("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"))
    assert(e.storage.inputFormat.contains("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"))
    assert(e.storage.outputFormat.contains("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"))
    assert(e.storage.columns == Seq(GlueColumn("id", "bigint"), GlueColumn("name", "string")))
    assert(e.partitionKeys == Seq(GlueColumn("dt", "string")))
    assert(e.storage.location.contains(loc))
    assert(e.storage.serdeParameters("path") == loc)
    assert(!e.parameters.contains("table_type"))
    assert(e.parameters("EXTERNAL") == "TRUE")
  }

  private def assertSparkSpecific(e: io.github.sdaberdaku.gluecat.glue.GlueTableEntry): Unit = {
    assert(e.storage.columns == Seq(GlueColumn("col", "array<string>", Some("from deserializer"))))
    assert(e.storage.location.exists(_.endsWith("-__PLACEHOLDER__")))
    assert(e.storage.serdeParameters("path") == loc)
    assert(e.storage.serdeLibrary.contains("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
    assert(e.storage.inputFormat.contains("org.apache.hadoop.mapred.SequenceFileInputFormat"))
    assert(e.storage.outputFormat.contains("org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat"))
    assert(e.partitionKeys == Seq(GlueColumn("dt", "string")))
  }

  test("ANSI interval column falls back to the Spark-specific form") {
    val s = new StructType().add("id", "string").add("i", DayTimeIntervalType()).add("dt", "string")
    assertSparkSpecific(EntryConverter.toGlueTable(ct("parquet", s, Seq("dt"), loc), 4000))
  }

  test("nested field name needing quoting falls back to the Spark-specific form") {
    val s = new StructType().add("id", "string").add("n", new StructType().add("a-b", "int")).add("dt", "string")
    assertSparkSpecific(EntryConverter.toGlueTable(ct("parquet", s, Seq("dt"), loc), 4000))
  }

  test("create version is kept when set, else the running Spark version") {
    val kept = EntryConverter.toGlueTable(ct("delta", idDt, Nil, loc).copy(createVersion = "3.5.5"), 4000)
    assert(kept.parameters("spark.sql.create.version") == "3.5.5")
    val unset = EntryConverter.toGlueTable(ct("delta", idDt, Nil, loc).copy(createVersion = ""), 4000)
    assert(unset.parameters("spark.sql.create.version") == SPARK_VERSION)
  }

  test("parquet with timestamp_ntz falls back to the Spark-specific form") {
    val s = new StructType().add("id", "string").add("ts", TimestampNTZType).add("dt", "string")
    assertSparkSpecific(EntryConverter.toGlueTable(ct("parquet", s, Seq("dt"), loc), 4000))
  }

  test("json uses the Spark-specific form") {
    assertSparkSpecific(EntryConverter.toGlueTable(ct("json", idDt, Seq("dt"), loc), 4000))
  }

  test("isHiveCompatibleType rejects timestamp_ntz nested anywhere") {
    assert(EntryConverter.isHiveCompatibleType(StringType))
    assert(!EntryConverter.isHiveCompatibleType(TimestampNTZType))
    assert(!EntryConverter.isHiveCompatibleType(ArrayType(TimestampNTZType)))
    assert(!EntryConverter.isHiveCompatibleType(MapType(StringType, new StructType().add("a", TimestampNTZType))))
    assert(EntryConverter.isHiveCompatibleType(MapType(StringType, new StructType().add("a", IntegerType))))
  }

  private def schemaOf(names: Seq[String]): StructType = names.foldLeft(new StructType())(_.add(_, "string"))

  private def parts(e: io.github.sdaberdaku.gluecat.glue.GlueTableEntry): Seq[String] =
    (0 until e.parameters("spark.sql.sources.schema.numParts").toInt)
      .map(i => e.parameters(s"spark.sql.sources.schema.part.$i"))

  test("schema longer than the threshold is split") {
    val schema = schemaOf((0 until 70).map(i => s"c$i"))
    assert(schema.json.length > 4000)
    val e = EntryConverter.toGlueTable(ct("parquet", schema, Nil, loc), 4000)
    assert(!e.parameters.contains("spark.sql.sources.schema"))
    val ps = parts(e)
    assert(ps.size == math.ceil(schema.json.length / 4000.0).toInt)
    assert(ps.mkString == schema.json)
    assert(ps.forall(_.length <= 4000))
    assert(ps.init.forall(_.length == 4000))
  }

  test("schema split boundaries") {
    val base = schemaOf(Seq("x")).json.length - 1
    val schema = schemaOf(Seq("x" * (4001 - base)))
    assert(schema.json.length == 4001)
    val single = EntryConverter.toGlueTable(ct("parquet", schema, Nil, loc), 4001)
    assert(single.parameters("spark.sql.sources.schema") == schema.json)
    assert(!single.parameters.contains("spark.sql.sources.schema.numParts"))
    val split = EntryConverter.toGlueTable(ct("parquet", schema, Nil, loc), 4000)
    assert(!split.parameters.contains("spark.sql.sources.schema"))
    assert(parts(split).map(_.length) == Seq(4000, 1))
  }

  test("read after write round trip, including a location with a space") {
    for (p <- Seq("delta", "parquet", "json"); l <- Seq(loc, "s3://bucket-a/my dir/t")) {
      val in = ct(p, idNameDt, Seq("dt"), l, Map("k" -> "v")).copy(createVersion = "3.5.5")
      val out = EntryConverter.toCatalogTable("acct_a", EntryConverter.toGlueTable(in, 4000))
      assert(out.schema == in.schema, p)
      assert(out.partitionColumnNames == in.partitionColumnNames, p)
      assert(out.provider == in.provider, p)
      assert(out.storage.locationUri == in.storage.locationUri, p)
      assert(out.properties == in.properties, p)
      assert(out.createVersion == "3.5.5", p)
    }
  }

  test("comment and owner") {
    val e = EntryConverter.toGlueTable(ct("delta", idDt, Nil, loc, comment = Some("c"), owner = "o"), 4000)
    assert(e.description.contains("c"))
    assert(e.owner.contains("o"))
  }

  test("namespace conversions") {
    val md = Map("comment" -> "c", "location" -> "s3://bucket-a/db", "k" -> "v").asJava
    val db = EntryConverter.toGlueDatabase("db", md)
    assert(db == GlueDatabaseEntry("db", Some("c"), Some("s3://bucket-a/db"), Map("k" -> "v")))
    assert(EntryConverter.toNamespaceMetadata(db) == md)
    assert(EntryConverter.toNamespaceMetadata(GlueDatabaseEntry("d", None, None, Map("k" -> "v"))).asScala ==
      Map("k" -> "v"))
  }

  test("partitionProvider follows tracksPartitionsInCatalog") {
    val base = ct("parquet", idNameDt, Seq("dt"), "s3://bucket-a/parquet/t")
    assert(EntryConverter.toGlueTable(base, 4000).parameters("spark.sql.partitionProvider") == "filesystem")
    assert(EntryConverter.toGlueTable(base.copy(tracksPartitionsInCatalog = true), 4000)
      .parameters("spark.sql.partitionProvider") == "catalog")
  }
}
