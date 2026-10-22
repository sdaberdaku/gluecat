package io.github.sdaberdaku.gluecat.catalog

import java.net.URI
import java.util

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.connector.catalog.{Table, TableCapability}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class V1TableBridgeSuite extends AnyFunSuite {
  private val ct = CatalogTable(
    identifier = TableIdentifier("t", Some("db"), Some("acct_a")),
    tableType = CatalogTableType.EXTERNAL,
    storage = CatalogStorageFormat.empty.copy(locationUri = Some(URI.create("s3://b/t"))),
    schema = StructType(Seq(StructField("a", IntegerType))),
    provider = Some("parquet"))

  private val someOtherTableStub: Table = new Table {
    override def name(): String = "other"
    override def schema(): StructType = new StructType()
    override def capabilities(): util.Set[TableCapability] = util.Collections.emptySet()
  }

  test("wrap exposes name and schema") {
    // Spark qualifies the name ("db.t"), so only the table part is asserted
    assert(V1TableBridge.wrap(ct).name().split("\\.").last == "t")
    assert(V1TableBridge.wrap(ct).schema() == ct.schema)
  }
  test("unwrap round-trips") {
    assert(V1TableBridge.unwrap(V1TableBridge.wrap(ct)) == Some(ct))
  }
  test("unwrap of another table is None") {
    assert(V1TableBridge.unwrap(someOtherTableStub).isEmpty)
  }
}
