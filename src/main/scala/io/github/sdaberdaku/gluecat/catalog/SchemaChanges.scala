package io.github.sdaberdaku.gluecat.catalog

import org.apache.spark.sql.connector.catalog.TableChange
import org.apache.spark.sql.connector.catalog.TableChange._
import org.apache.spark.sql.types.{StructField, StructType}

object SchemaChanges {
  def apply(schema: StructType, changes: Seq[TableChange]): StructType = changes.foldLeft(schema)(applyOne)

  private def top(c: TableChange, names: Array[String]): String = {
    if (names.length != 1) {
      throw new UnsupportedOperationException(s"${c.getClass.getSimpleName} on nested column ${names.mkString(".")}")
    }
    names(0)
  }

  private def update(schema: StructType, name: String)(f: StructField => StructField): StructType = {
    if (!schema.fieldNames.contains(name)) throw new IllegalArgumentException(s"Column not found: $name")
    StructType(schema.fields.map(fl => if (fl.name == name) f(fl) else fl))
  }

  private def applyOne(schema: StructType, change: TableChange): StructType = change match {
    case c: AddColumn =>
      val name = top(c, c.fieldNames())
      val field = StructField(name, c.dataType(), c.isNullable)
      schema.add(Option(c.comment()).fold(field)(field.withComment))
    case c: DeleteColumn =>
      val name = top(c, c.fieldNames())
      update(schema, name)(identity)
      StructType(schema.fields.filterNot(_.name == name))
    case c: RenameColumn => update(schema, top(c, c.fieldNames()))(_.copy(name = c.newName()))
    case c: UpdateColumnType => update(schema, top(c, c.fieldNames()))(_.copy(dataType = c.newDataType()))
    case c: UpdateColumnComment => update(schema, top(c, c.fieldNames()))(_.withComment(c.newComment()))
    case c: UpdateColumnNullability => update(schema, top(c, c.fieldNames()))(_.copy(nullable = c.nullable()))
    case other =>
      throw new UnsupportedOperationException(s"Unsupported schema change: ${other.getClass.getSimpleName}")
  }
}
