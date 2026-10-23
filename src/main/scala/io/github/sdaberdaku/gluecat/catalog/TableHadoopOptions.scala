package io.github.sdaberdaku.gluecat.catalog

import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.TableCatalog
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

  /** For a staged write (CTAS, REPLACE ... AS SELECT): the caller's write options win, case-insensitively. SQL OPTIONS
    * arrive as `option.<key>` stage properties with no write options; they are the caller's then, as Delta itself
    * takes them only while the write options are empty. */
  def writeOptions(info: LogicalWriteInfo, stageProperties: java.util.Map[String, String]): LogicalWriteInfo =
    if (values.isEmpty) info
    else {
      val given = info.options().asCaseSensitiveMap().asScala.toMap
      val caller = if (given.nonEmpty) given else stageProperties.asScala.collect {
        case (k, v) if k.startsWith(TableCatalog.OPTION_PREFIX) => k.stripPrefix(TableCatalog.OPTION_PREFIX) -> v
      }.toMap
      val callerKeys = caller.keySet.map(_.toLowerCase(Locale.ROOT))
      val merged = new CaseInsensitiveStringMap(
        (values.filter { case (k, _) => !callerKeys(k.toLowerCase(Locale.ROOT)) } ++ caller).asJava)
      new LogicalWriteInfo {
        override def options(): CaseInsensitiveStringMap = merged
        override def queryId(): String = info.queryId()
        override def schema(): StructType = info.schema()
        override def rowIdSchema(): java.util.Optional[StructType] = info.rowIdSchema()
        override def metadataSchema(): java.util.Optional[StructType] = info.metadataSchema()
      }
    }

  private def sessionConf(): Configuration = hadoopConf(SparkSession.active.sessionState.newHadoopConf())

  /** The location's FileSystem, opened with the session's Hadoop configuration plus these options. */
  def fileSystem(location: String): FileSystem = new Path(location).getFileSystem(sessionConf())

  /** Opens the location's FileSystem with these options before Delta opens it with the session configuration, which
    * gets the cached instance (cache-dependent by design: a bucket opened earlier with the session configuration
    * keeps it). Without the cache for the scheme, priming would only leave an instance open. */
  def prime(location: String): Unit = if (values.nonEmpty) {
    val conf = sessionConf()
    val path = new Path(location)
    val scheme = Option(path.toUri.getScheme).getOrElse(FileSystem.getDefaultUri(conf).getScheme)
    if (!conf.getBoolean(s"fs.$scheme.impl.disable.cache", false)) path.getFileSystem(conf)
  }
}
