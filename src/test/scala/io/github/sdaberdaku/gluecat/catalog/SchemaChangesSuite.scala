package io.github.sdaberdaku.gluecat.catalog

import org.apache.spark.sql.connector.catalog.TableChange
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SchemaChangesSuite extends AnyFunSuite {
  private val schema = StructType(Seq(StructField("a", IntegerType), StructField("b", StringType)))
  private def apply(c: TableChange) = SchemaChanges(schema, Seq(c))

  test("add column appends") {
    assert(apply(TableChange.addColumn(Array("c"), LongType)).fieldNames.toSeq == Seq("a", "b", "c"))
  }
  test("delete column") {
    assert(apply(TableChange.deleteColumn(Array("a"), false)).fieldNames.toSeq == Seq("b"))
  }
  test("rename column") {
    assert(apply(TableChange.renameColumn(Array("a"), "z")).fieldNames.toSeq == Seq("z", "b"))
  }
  test("update column type") {
    assert(apply(TableChange.updateColumnType(Array("a"), LongType))("a").dataType == LongType)
  }
  test("update column comment") {
    assert(apply(TableChange.updateColumnComment(Array("a"), "x"))("a").getComment() == Some("x"))
  }
  test("update column nullability") {
    assert(!apply(TableChange.updateColumnNullability(Array("a"), false))("a").nullable)
  }
  test("nested paths are unsupported") {
    intercept[UnsupportedOperationException](apply(TableChange.addColumn(Array("s", "x"), LongType)))
  }
  test("position changes are unsupported") {
    intercept[UnsupportedOperationException](
      apply(TableChange.updateColumnPosition(Array("b"), TableChange.ColumnPosition.first())))
  }
  test("deleting a missing column names it") {
    val e = intercept[IllegalArgumentException](apply(TableChange.deleteColumn(Array("nope"), false)))
    assert(e.getMessage.contains("nope"))
  }
}
