package io.github.sdaberdaku.gluecat.catalog

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.Table

/** Reflection around Spark's private[sql] V1Table, which gives V2 catalogs access to the V1 read/write paths. */
object V1TableBridge {
  private val v1TableClass = Class.forName("org.apache.spark.sql.connector.catalog.V1Table")
  private val ctor = v1TableClass.getConstructor(classOf[CatalogTable])
  private val v1TableMethod = v1TableClass.getMethod("v1Table")

  def wrap(table: CatalogTable): Table = ctor.newInstance(table).asInstanceOf[Table]

  def unwrap(table: Table): Option[CatalogTable] =
    if (v1TableClass.isInstance(table)) Some(v1TableMethod.invoke(table).asInstanceOf[CatalogTable]) else None
}
