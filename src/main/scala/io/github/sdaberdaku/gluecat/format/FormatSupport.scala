package io.github.sdaberdaku.gluecat.format

import org.apache.spark.sql.connector.catalog.{Identifier, StagedTable, Table, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType

/** The table operations whose behaviour depends on the table format. */
trait FormatSupport {
  def loadTable(ident: Identifier): Table
  def loadTable(ident: Identifier, version: String): Table
  def loadTable(ident: Identifier, timestamp: Long): Table
  def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform],
      properties: java.util.Map[String, String]): Table
  def stageCreate(ident: Identifier, schema: StructType, partitions: Array[Transform],
      properties: java.util.Map[String, String]): StagedTable
  def stageReplace(ident: Identifier, schema: StructType, partitions: Array[Transform],
      properties: java.util.Map[String, String]): StagedTable
  def stageCreateOrReplace(ident: Identifier, schema: StructType, partitions: Array[Transform],
      properties: java.util.Map[String, String]): StagedTable
  def alterTable(ident: Identifier, changes: Seq[TableChange]): Table
  /** REFRESH TABLE; re-syncs the Glue columns of Delta tables. */
  def invalidateTable(ident: Identifier): Unit
}
