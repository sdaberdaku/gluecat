package io.github.sdaberdaku.gluecat.format

import org.apache.spark.sql.connector.catalog.{Identifier, StagedTable, Table, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType

import io.github.sdaberdaku.gluecat.catalog.GlueCoreCatalog

/** Every table goes straight to the core catalog: metadata only, no time travel, no CTAS or REPLACE. */
class PassThroughFormatSupport(core: GlueCoreCatalog) extends FormatSupport {
  override def loadTable(ident: Identifier): Table = core.loadTable(ident)

  // Load first so that a missing table still reports NoSuchTableException.
  override def loadTable(ident: Identifier, version: String): Table = noTimeTravel(ident)
  override def loadTable(ident: Identifier, timestamp: Long): Table = noTimeTravel(ident)

  private def noTimeTravel(ident: Identifier): Table = {
    core.loadTable(ident)
    throw new UnsupportedOperationException("Time travel is supported for Delta tables only")
  }

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform],
      properties: java.util.Map[String, String]): Table = core.createTable(ident, schema, partitions, properties)

  override def stageCreate(ident: Identifier, schema: StructType, partitions: Array[Transform],
      properties: java.util.Map[String, String]): StagedTable = noStaging()
  override def stageReplace(ident: Identifier, schema: StructType, partitions: Array[Transform],
      properties: java.util.Map[String, String]): StagedTable = noStaging()
  override def stageCreateOrReplace(ident: Identifier, schema: StructType, partitions: Array[Transform],
      properties: java.util.Map[String, String]): StagedTable = noStaging()

  private def noStaging(): Nothing =
    throw new UnsupportedOperationException("CREATE TABLE AS SELECT and REPLACE TABLE are supported for Delta tables only")

  override def alterTable(ident: Identifier, changes: Seq[TableChange]): Table = core.alterTable(ident, changes: _*)

  override def invalidateTable(ident: Identifier): Unit = ()
}
