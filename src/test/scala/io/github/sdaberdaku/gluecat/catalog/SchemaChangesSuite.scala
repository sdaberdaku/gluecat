package io.github.sdaberdaku.gluecat.catalog

import org.apache.spark.sql.connector.catalog.{ColumnDefaultValue, TableChange}
import org.apache.spark.sql.connector.expressions.Expressions
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SchemaChangesSuite extends AnyFunSuite {
  private val schema = StructType(Seq(StructField("a", IntegerType), StructField("b", StringType)))
  private def apply(c: TableChange) = SchemaChanges(schema, Seq(c))

  // The positional addColumn factory has a different arity on Spark 3.5 and 4.x (4.x adds a default value).
  private def addAt(pos: TableChange.ColumnPosition): TableChange = {
    val m = classOf[TableChange].getMethods.filter(x => x.getName == "addColumn" && x.getParameterCount >= 5)
      .minBy(_.getParameterCount)
    val args = Array[AnyRef](Array("c"), LongType, java.lang.Boolean.TRUE, null, pos) ++
      Array.fill[AnyRef](m.getParameterCount - 5)(null)
    m.invoke(null, args: _*).asInstanceOf[TableChange]
  }

  test("add column appends") {
    assert(apply(TableChange.addColumn(Array("c"), LongType)).fieldNames.toSeq == Seq("a", "b", "c"))
  }
  test("add column first") {
    val c = addAt(TableChange.ColumnPosition.first())
    assert(apply(c).fieldNames.toSeq == Seq("c", "a", "b"))
  }
  test("add column after") {
    val c = addAt(TableChange.ColumnPosition.after("a"))
    assert(apply(c).fieldNames.toSeq == Seq("a", "c", "b"))
  }
  test("add column after a missing column names it") {
    val c = addAt(TableChange.ColumnPosition.after("missing"))
    assert(intercept[IllegalArgumentException](apply(c)).getMessage.contains("missing"))
  }
  test("add column with a default value is refused naming the column") {
    val c = TableChange.addColumn(Array("c"), LongType, true, null, null,
      new ColumnDefaultValue("1", Expressions.literal(java.lang.Long.valueOf(1L))))
    assert(intercept[UnsupportedOperationException](apply(c)).getMessage.contains("column c"))
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
