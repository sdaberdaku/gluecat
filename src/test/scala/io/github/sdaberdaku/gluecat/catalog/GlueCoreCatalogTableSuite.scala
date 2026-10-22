package io.github.sdaberdaku.gluecat.catalog

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.catalog.{Identifier, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import org.scalatest.funsuite.AnyFunSuite

import io.github.sdaberdaku.gluecat.GlueCatalogOptions
import io.github.sdaberdaku.gluecat.glue._

@RunWith(classOf[JUnitRunner])
class GlueCoreCatalogTableSuite extends AnyFunSuite {
  private def opts(extra: (String, String)*) =
    GlueCatalogOptions.parse("acct_a", (Map("glue.id" -> "111111111111") ++ extra).asJava)
  private def fixture(extra: (String, String)*) = {
    val glue = new InMemoryGlueApi
    val c = new GlueCoreCatalog("acct_a", glue, opts(extra: _*), () => 4000)
    c.createNamespace(Array("db"), Map("location" -> "s3://bucket-a/db/").asJava)
    (c, glue)
  }
  private val schema = StructType(Seq(StructField("id", IntegerType)))
  private val noParts = Array.empty[Transform]
  private def props(kv: (String, String)*) = kv.toMap.asJava
  private def id(name: String) = Identifier.of(Array("db"), name)
  private def create(c: GlueCoreCatalog, name: String = "t", provider: String = "parquet") =
    c.createTable(id(name), schema, noParts, props("provider" -> provider, "location" -> s"s3://bucket-a/$name"))
  private def v1(c: GlueCoreCatalog, name: String = "t") =
    V1TableBridge.unwrap(c.loadTable(id(name))).get

  test("createTable then loadTable") {
    val (c, _) = fixture()
    create(c)
    val t = v1(c)
    assert(t.provider.contains("parquet"))
    assert(t.storage.locationUri.map(_.toString).contains("s3://bucket-a/t"))
    assert(t.identifier.catalog.contains("acct_a"))
  }

  test("managed location from the database location without double slash") {
    val (c, _) = fixture()
    val spec = TableProperties.split(props(), "parquet")
    assert(c.resolveLocation(id("t"), spec) == "s3://bucket-a/db/t")
    val (c2, _) = fixture("warehouse" -> "s3://bucket-a/wh/")
    c2.createNamespace(Array("db2"), props())
    assert(c2.resolveLocation(Identifier.of(Array("db2"), "t"), spec) == "s3://bucket-a/wh/db2.db/t")
    val (c3, _) = fixture()
    c3.createNamespace(Array("db2"), props())
    val e = intercept[IllegalArgumentException](c3.resolveLocation(Identifier.of(Array("db2"), "t"), spec))
    assert(e.getMessage.contains("warehouse"))
  }

  test("identifiers are lowercased") {
    val (c, _) = fixture()
    c.createNamespace(Array("MyDb"), props("location" -> "s3://bucket-a/mydb"))
    c.createTable(Identifier.of(Array("MyDb"), "MyTable"), schema, noParts, props())
    assert(c.tableExists(Identifier.of(Array("MYDB"), "mytable")))
    assert(c.listTables(Array("MyDb")).toSeq == Seq(Identifier.of(Array("mydb"), "mytable")))
  }

  test("listTables skips views") {
    val (c, glue) = fixture()
    val storage = GlueStorage(None, None, None, None, Map.empty, Nil)
    glue.createTable(GlueTableEntry("db", "v", Some("VIRTUAL_VIEW"), None, None, Map.empty, storage, Nil, None))
    glue.createTable(GlueTableEntry("db", "ice", Some("EXTERNAL_TABLE"), None, None,
      Map("table_type" -> "ICEBERG"), storage, Nil, None))
    assert(c.listTables(Array("db")).map(_.name).toSeq == Seq("ice"))
    intercept[NoSuchNamespaceException](c.listTables(Array("nope")))
  }

  test("createTable twice and in a missing namespace") {
    val (c, _) = fixture()
    create(c)
    intercept[TableAlreadyExistsException](create(c))
    intercept[NoSuchNamespaceException](
      c.createTable(Identifier.of(Array("nope"), "t"), schema, noParts, props("location" -> "s3://bucket-a/x")))
  }

  test("loadTable missing") {
    val (c, _) = fixture()
    intercept[NoSuchTableException](c.loadTable(id("t")))
    assert(!c.tableExists(id("t")))
  }

  test("alterTable properties") {
    val (c, glue) = fixture()
    c.createTable(id("t"), schema, noParts,
      props("provider" -> "parquet", "location" -> "s3://bucket-a/t", "gone" -> "1"))
    val e = glue.getTable("db", "t").get
    glue.updateTable(e.copy(parameters = e.parameters + ("trino_last_transaction_version" -> "5")), None, true)
    // the update above made the entry version 2
    c.alterTable(id("t"), TableChange.setProperty("k", "v"), TableChange.removeProperty("gone"),
      TableChange.setProperty("comment", "c"), TableChange.setProperty("location", "s3://bucket-a/t2"))
    val after = glue.getTable("db", "t").get
    assert(after.parameters("k") == "v" && !after.parameters.contains("gone"))
    assert(after.parameters("trino_last_transaction_version") == "5")
    assert(after.description.contains("c"))
    assert(EntryConverter.location(after).contains("s3://bucket-a/t2"))
    assert(after.versionId.contains("3"))
  }

  test("alterTable bumps the version from 1 to 2") {
    val (c, glue) = fixture()
    create(c)
    c.alterTable(id("t"), TableChange.setProperty("k", "v"))
    assert(glue.getTable("db", "t").get.versionId.contains("2"))
  }

  test("alterTable columns") {
    val (c, glue) = fixture()
    create(c, "p")
    c.alterTable(id("p"), TableChange.addColumn(Array("x"), StringType))
    assert(v1(c, "p").schema.fieldNames.toSeq == Seq("id", "x"))
    assert(glue.getTable("db", "p").get.storage.columns.map(_.name) == Seq("id", "x"))
    create(c, "j", "json")
    c.alterTable(id("j"), TableChange.addColumn(Array("x"), StringType))
    assert(glue.getTable("db", "j").get.parameters("spark.sql.sources.schema").contains("\"x\""))
    assert(v1(c, "j").schema.fieldNames.toSeq == Seq("id", "x"))
  }

  test("alterTable reserved keys fail") {
    val (c, _) = fixture()
    create(c)
    intercept[UnsupportedOperationException](c.alterTable(id("t"), TableChange.setProperty("provider", "orc")))
    intercept[UnsupportedOperationException](c.alterTable(id("t"), TableChange.setProperty("external", "true")))
  }

  test("collation is rejected") {
    val (c, _) = fixture()
    val e = intercept[UnsupportedOperationException](c.createTable(id("t"), schema, noParts,
      props("location" -> "s3://bucket-a/t", "collation" -> "UTF8_LCASE")))
    assert(e.getMessage.contains("collation"))
    create(c)
    val e2 = intercept[UnsupportedOperationException](c.alterTable(id("t"), TableChange.setProperty("collation", "x")))
    assert(e2.getMessage.contains("collation"))
  }

  test("alterTable passes the VersionId and surfaces conflicts") {
    val (c, glue) = fixture()
    create(c)
    glue.failNext = Some(("UpdateTable", new GlueConcurrentModification("stale")))
    val e = intercept[GlueCatalogConcurrentModificationException](
      c.alterTable(id("t"), TableChange.setProperty("k", "v")))
    assert(e.getMessage.contains("VersionId 1"))
  }

  test("renameTable") {
    val (c, glue) = fixture()
    create(c)
    c.renameTable(id("t"), id("t2"))
    assert(c.tableExists(id("t2")) && !c.tableExists(id("t")))
    assert(v1(c, "t2").storage.locationUri.map(_.toString).contains("s3://bucket-a/t"))

    create(c, "a")
    create(c, "b")
    intercept[TableAlreadyExistsException](c.renameTable(id("a"), id("b")))
    assert(c.tableExists(id("a")) && c.tableExists(id("b")))

    glue.failNext = Some(("DeleteTable", new GlueFailure("DeleteTable", "boom")))
    val e = intercept[RuntimeException](c.renameTable(id("a"), id("a2")))
    assert(e.getMessage.contains("db.a") && e.getMessage.contains("db.a2"))
    assert(c.tableExists(id("a")) && c.tableExists(id("a2")))
  }

  test("dropTable and purgeTable") {
    val (c, glue) = fixture()
    create(c)
    assert(c.dropTable(id("t")))
    assert(glue.getTable("db", "t").isEmpty)
    assert(!c.dropTable(id("t")))
    val e = intercept[UnsupportedOperationException](c.purgeTable(id("t")))
    assert(e.getMessage.contains("PURGE"))
  }

  test("registerTable and replaceTable") {
    val (c, glue) = fixture()
    create(c)
    val table = v1(c)
    c.registerTable(id("u"), table.copy(identifier = table.identifier.copy(database = Some("Db"), table = "U")))
    assert(glue.getTable("db", "u").exists(e => e.database == "db" && e.name == "u"))
    val e = glue.getTable("db", "t").get
    glue.updateTable(e.copy(parameters = e.parameters + ("trino_x" -> "1")), None, true)
    c.replaceTable(id("t"), table.copy(schema = schema.add("y", StringType)))
    val after = glue.getTable("db", "t").get
    assert(after.parameters("trino_x") == "1")
    assert(after.versionId.contains("3"))
    assert(v1(c).schema.fieldNames.toSeq == Seq("id", "y"))
  }

  test("alter rewrites TableType to EXTERNAL_TABLE") {
    val (c, glue) = fixture()
    create(c)
    val e = glue.getTable("db", "t").get
    glue.updateTable(e.copy(tableType = Some("MANAGED_TABLE")), None, true)
    c.alterTable(id("t"), TableChange.setProperty("k", "v"))
    assert(glue.getTable("db", "t").get.tableType.contains("EXTERNAL_TABLE"))
  }

  test("alter keeps bucket-spec parameters") {
    val (c, glue) = fixture()
    create(c)
    val e = glue.getTable("db", "t").get
    glue.updateTable(e.copy(parameters = e.parameters ++ Map("spark.sql.sources.schema.numBuckets" -> "4",
      "spark.sql.sources.schema.bucketCol.0" -> "id")), None, true)
    c.alterTable(id("t"), TableChange.setProperty("k", "v"))
    val p = glue.getTable("db", "t").get.parameters
    assert(p("spark.sql.sources.schema.numBuckets") == "4" && p("spark.sql.sources.schema.bucketCol.0") == "id")
  }

  test("catalog-owned keys cannot be set, unset or created") {
    val (c, _) = fixture()
    create(c)
    intercept[UnsupportedOperationException](c.alterTable(id("t"), TableChange.setProperty("table_type", "ICEBERG")))
    val e = intercept[UnsupportedOperationException](
      c.alterTable(id("t"), TableChange.removeProperty("spark.sql.sources.provider")))
    assert(e.getMessage.contains("spark.sql.sources.provider"))
    intercept[UnsupportedOperationException](c.createTable(id("u"), schema, noParts,
      props("location" -> "s3://bucket-a/u", "spark.sql.foo" -> "1")))
  }

  test("views cannot be dropped, renamed, altered or replaced") {
    val (c, glue) = fixture()
    create(c)
    val tbl = v1(c)
    glue.createTable(GlueTableEntry("db", "v", Some("VIRTUAL_VIEW"), None, None, Map.empty,
      GlueStorage(None, None, None, None, Map.empty, Nil), Nil, None))
    intercept[UnsupportedTableException](c.dropTable(id("v")))
    intercept[UnsupportedTableException](c.renameTable(id("v"), id("w")))
    intercept[UnsupportedTableException](c.alterTable(id("v"), TableChange.setProperty("k", "v")))
    intercept[UnsupportedTableException](c.replaceTable(id("v"), tbl))
    assert(glue.getTable("db", "v").isDefined)
  }

  test("rename with an unqualified target stays in the source namespace") {
    val (c, _) = fixture()
    create(c)
    c.renameTable(id("t"), Identifier.of(Array.empty[String], "t2"))
    assert(c.tableExists(id("t2")) && !c.tableExists(id("t")))
  }
}
