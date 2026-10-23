package io.github.sdaberdaku.gluecat

import scala.util.control.NonFatal

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import io.github.sdaberdaku.gluecat.catalog.{DefaultNamespaceNotFoundException, GlueCoreCatalog, TableHadoopOptions}
import io.github.sdaberdaku.gluecat.format.{DeltaFormatSupport, FormatSupport, PassThroughFormatSupport}
import io.github.sdaberdaku.gluecat.glue.{GlueApi, SdkGlueApi}

/** The catalog plugin users configure. Routes every call to the core catalog or to the format support.
  * Never a CatalogExtension, so it also works as `spark_catalog` (see the design). */
class GlueCatalog extends TableCatalog with StagingTableCatalog with SupportsNamespaces {
  private val log = LoggerFactory.getLogger(getClass)
  private var catalogName: String = _
  private var glueOptions: GlueCatalogOptions = _
  // Kept for format supports that initialise a catalog of their own (Delta).
  private var sparkOptions: CaseInsensitiveStringMap = _
  private var hadoopOptions: TableHadoopOptions = _
  private var core: GlueCoreCatalog = _
  private var format: FormatSupport = _

  override def name(): String = catalogName

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogName = name
    sparkOptions = options
    // Spark reports exceptions escaping here without the catalog name, or as "cannot find catalog plugin class".
    try {
      SparkVersionCheck.requireSupported(SPARK_VERSION)
      glueOptions = GlueCatalogOptions.parse(name, options.asCaseSensitiveMap)
      DeltaFormatSupport.requireUpdateCatalogHookOff()
      S3RoleDefaults.warnings(name, glueOptions).foreach(w => log.warn(w))
      hadoopOptions = TableHadoopOptions(S3RoleDefaults.effective(glueOptions,
        SessionHadoop(SparkSession.active.sessionState.newHadoopConf())))
      // Keys only: values may hold credentials.
      val keys = hadoopOptions.values.keys.toSeq.sorted
      if (keys.nonEmpty) log.info(s"gluecat catalog '$name': per-table Hadoop options: ${keys.mkString(", ")}")
      core = new GlueCoreCatalog(name, newGlueApi(glueOptions), glueOptions,
        () => SparkSession.active.conf.get("spark.sql.sources.schemaStringLengthThreshold", "4000").toInt,
        hadoopOptions)
      format = newFormatSupport(core, glueOptions)
    } catch {
      case NonFatal(e) => throw new IllegalArgumentException(s"gluecat catalog '$name': ${e.getMessage}", e)
    }
  }

  protected def newGlueApi(options: GlueCatalogOptions): GlueApi = SdkGlueApi(options)

  protected def newFormatSupport(core: GlueCoreCatalog, options: GlueCatalogOptions): FormatSupport =
    if (DeltaFormatSupport.available) new DeltaFormatSupport(core, catalogName, options, sparkOptions, hadoopOptions)
    else new PassThroughFormatSupport(core)

  // ---- SupportsNamespaces ----

  override def defaultNamespace(): Array[String] = core.defaultNamespace()
  override def listNamespaces(): Array[Array[String]] = core.listNamespaces()
  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = core.listNamespaces(namespace)
  override def namespaceExists(namespace: Array[String]): Boolean = core.namespaceExists(namespace)
  override def loadNamespaceMetadata(namespace: Array[String]): java.util.Map[String, String] =
    core.loadNamespaceMetadata(namespace)
  override def createNamespace(namespace: Array[String], metadata: java.util.Map[String, String]): Unit =
    core.createNamespace(namespace, metadata)
  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit =
    core.alterNamespace(namespace, changes: _*)
  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean =
    core.dropNamespace(namespace, cascade)

  // ---- TableCatalog ----

  override def listTables(namespace: Array[String]): Array[Identifier] = core.listTables(namespace)
  override def tableExists(ident: Identifier): Boolean = core.tableExists(ident)

  override def loadTable(ident: Identifier): Table = load(ident)(format.loadTable(ident))
  override def loadTable(ident: Identifier, version: String): Table = load(ident)(format.loadTable(ident, version))
  override def loadTable(ident: Identifier, timestamp: Long): Table = load(ident)(format.loadTable(ident, timestamp))

  // A table missing from the default namespace because the database itself is missing: name the database
  // (spec "Default namespace"); Spark would only report the table.
  private def load(ident: Identifier)(f: => Table): Table =
    try f catch {
      case _: NoSuchTableException
          if core.normalize(ident.namespace).sameElements(core.defaultNamespace()) && !core.namespaceExists(ident.namespace) =>
        throw new DefaultNamespaceNotFoundException(catalogName, core.defaultNamespace().head,
          glueOptions.glueId.getOrElse("<default>"))
    }

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform],
      properties: java.util.Map[String, String]): Table = format.createTable(ident, schema, partitions, properties)

  override def stageCreate(ident: Identifier, schema: StructType, partitions: Array[Transform],
      properties: java.util.Map[String, String]): StagedTable = format.stageCreate(ident, schema, partitions, properties)

  override def stageReplace(ident: Identifier, schema: StructType, partitions: Array[Transform],
      properties: java.util.Map[String, String]): StagedTable = format.stageReplace(ident, schema, partitions, properties)

  override def stageCreateOrReplace(ident: Identifier, schema: StructType, partitions: Array[Transform],
      properties: java.util.Map[String, String]): StagedTable =
    format.stageCreateOrReplace(ident, schema, partitions, properties)

  override def alterTable(ident: Identifier, changes: TableChange*): Table = format.alterTable(ident, changes)
  override def invalidateTable(ident: Identifier): Unit = format.invalidateTable(ident)
  override def dropTable(ident: Identifier): Boolean = core.dropTable(ident)

  override def purgeTable(ident: Identifier): Boolean =
    throw new UnsupportedOperationException("PURGE is not supported: dropping a table never deletes its data")

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = core.renameTable(oldIdent, newIdent)
}
