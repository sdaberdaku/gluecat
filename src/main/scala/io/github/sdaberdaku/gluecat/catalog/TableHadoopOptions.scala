package io.github.sdaberdaku.gluecat.catalog

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** The catalog's per-table Hadoop options. They go on the objects handed to Spark and Delta, never into Glue or a
  * table's metadata (Delta persists `option.*` create properties in its log, so creates never get them). */
final case class TableHadoopOptions(values: Map[String, String]) {
  /** On load: the table's own storage properties (its OPTIONS) win. */
  def apply(table: CatalogTable): CatalogTable =
    table.copy(storage = table.storage.copy(properties = values ++ table.storage.properties))

  def hadoopConf(base: Configuration): Configuration = {
    val conf = new Configuration(base)
    values.foreach { case (k, v) => conf.set(k, v) }
    conf
  }

  /** For a staged write (CTAS, REPLACE ... AS SELECT): the caller's write options win, case-insensitively. */
  def writeOptions(info: LogicalWriteInfo): LogicalWriteInfo = {
    val caller = info.options()
    val merged = new CaseInsensitiveStringMap(
      (values.filter { case (k, _) => !caller.containsKey(k) } ++ caller.asCaseSensitiveMap().asScala).asJava)
    new LogicalWriteInfo {
      override def options(): CaseInsensitiveStringMap = merged
      override def queryId(): String = info.queryId()
      override def schema(): StructType = info.schema()
      override def rowIdSchema(): java.util.Optional[StructType] = info.rowIdSchema()
      override def metadataSchema(): java.util.Optional[StructType] = info.metadataSchema()
    }
  }

  /** The location's FileSystem, opened with the session's Hadoop configuration plus these options. */
  def fileSystem(location: String): FileSystem =
    new Path(location).getFileSystem(hadoopConf(SparkSession.active.sessionState.newHadoopConf()))

  /** Opens the location's FileSystem with these options before Delta opens it with the session configuration, which
    * gets the cached instance (cache-dependent by design: a bucket opened earlier with the session configuration
    * keeps it). */
  def prime(location: String): Unit = if (values.nonEmpty) fileSystem(location)
}
