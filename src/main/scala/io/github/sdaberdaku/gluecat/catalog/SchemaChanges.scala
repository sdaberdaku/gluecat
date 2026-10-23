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
      // The V1 schema has nowhere to keep it, so it would be dropped silently.
      if (c.defaultValue() != null) {
        throw new UnsupportedOperationException(s"ADD COLUMN ... DEFAULT is supported for Delta tables only (column $name)")
      }
      val field = StructField(name, c.dataType(), c.isNullable)
      val withComment = Option(c.comment()).fold(field)(field.withComment)
      c.position() match {
        case null => schema.add(withComment)
        case _: First => StructType(withComment +: schema.fields)
        case a: After =>
          val idx = schema.fieldNames.indexOf(a.column())
          if (idx < 0) throw new IllegalArgumentException(s"Column not found: ${a.column()}")
          val (head, tail) = schema.fields.splitAt(idx + 1)
          StructType((head :+ withComment) ++ tail)
        case other =>
          throw new UnsupportedOperationException(s"Unsupported column position: ${other.getClass.getSimpleName}")
      }
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
