package io.github.sdaberdaku.gluecat.catalog

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TableHadoopOptionsSuite extends AnyFunSuite {
  private val table = CatalogTable(TableIdentifier("t", Some("db")), CatalogTableType.EXTERNAL,
    CatalogStorageFormat.empty, new StructType())

  test("apply adds the options under the table's own") {
    val t = TableHadoopOptions(Map("fs.s3a.endpoint" -> "http://a", "fs.s3a.x" -> "1"))
      .apply(table.copy(storage = table.storage.copy(properties = Map("fs.s3a.endpoint" -> "http://user"))))
    assert(t.storage.properties == Map("fs.s3a.endpoint" -> "http://user", "fs.s3a.x" -> "1"))
  }

  private val writeSchema = new StructType().add("id", "int")
  private def writeInfo(kv: (String, String)*): LogicalWriteInfo = new LogicalWriteInfo {
    override def options(): CaseInsensitiveStringMap = new CaseInsensitiveStringMap(kv.toMap.asJava)
    override def queryId(): String = "q"
    override def schema(): StructType = writeSchema
  }
  private val catalogOptions = TableHadoopOptions(Map("fs.a" -> "catalog", "fs.b" -> "catalog", "fs.c" -> "catalog"))

  test("writeOptions puts the caller's options over the catalog's") {
    val merged = catalogOptions.writeOptions(writeInfo("fs.a" -> "caller", "FS.C" -> "caller", "mergeSchema" -> "true"),
      Map("option.fs.b" -> "ignored").asJava)
    assert(merged.options().asCaseSensitiveMap().asScala ==
      Map("fs.a" -> "caller", "fs.b" -> "catalog", "FS.C" -> "caller", "mergeSchema" -> "true"))
    assert(merged.queryId() == "q" && (merged.schema() eq writeSchema))
  }

  test("without write options, the SQL OPTIONS in the stage properties are the caller's") {
    val stage = Map("option.fs.a" -> "sql", "fs.a" -> "sql", "option.delta.appendOnly" -> "true",
      "delta.appendOnly" -> "true", "provider" -> "delta")
    val merged = catalogOptions.writeOptions(writeInfo(), stage.asJava)
    assert(merged.options().asCaseSensitiveMap().asScala ==
      Map("fs.a" -> "sql", "delta.appendOnly" -> "true", "fs.b" -> "catalog", "fs.c" -> "catalog"))
  }

  test("writeOptions without catalog options returns the write info as is") {
    val info = writeInfo()
    assert(TableHadoopOptions(Map.empty).writeOptions(info, Map("option.a" -> "1").asJava) eq info)
  }

  test("hadoopConf sets the values on a copy") {
    val c = new Configuration(false)
    val h = TableHadoopOptions(Map("fs.a" -> "1")).hadoopConf(c)
    assert(h.get("fs.a") == "1")
    assert(c.get("fs.a") == null)
  }
}
