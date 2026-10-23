package io.github.sdaberdaku.gluecat.format

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import io.github.sdaberdaku.gluecat.GlueCatalogOptions
import io.github.sdaberdaku.gluecat.catalog._
import io.github.sdaberdaku.gluecat.glue.{GlueAlreadyExists, GlueTableEntry}

/** Delta tables. Loads and alters go through Delta's own `DeltaCatalog` wrapped around the core. Creates and
  * replaces never pass a catalog identifier to Delta (it would register them in `spark_catalog`): they run on
  * the path identifier ``delta.`<location>` ``, then the Glue entry is written. */
class DeltaFormatSupport(core: GlueCoreCatalog, catalogName: String, options: GlueCatalogOptions,
    sparkOptions: CaseInsensitiveStringMap, hadoopOptions: TableHadoopOptions) extends FormatSupport {

  // Only Spark types here: DeltaCatalog's class differs between Delta 3.3 and 4.x.
  private val delta: DelegatingCatalogExtension = {
    val d = DeltaFormatSupport.deltaCatalogClass().getDeclaredConstructor().newInstance()
      .asInstanceOf[DelegatingCatalogExtension]
    d.setDelegateCatalog(new SchemalessDeltaView(core))
    d.initialize(catalogName, sparkOptions)
    d
  }
  private def staging = delta.asInstanceOf[StagingTableCatalog]

  def pathIdentifier(location: String): Identifier = Identifier.of(Array("delta"), location)

  // Every by-path call on a catalog's table: the location's FileSystem is opened with the catalog's options first.
  private def atPath(location: String): Identifier = { hadoopOptions.prime(location); pathIdentifier(location) }

  def isDelta(provider: String): Boolean = provider.equalsIgnoreCase("delta")
  private def isDeltaEntry(entry: GlueTableEntry): Boolean = entry.parameters.get("spark.sql.sources.provider").exists(isDelta)

  // ``delta.`<path>` `` reaches this catalog when it is spark_catalog: Delta handles it alone, Glue is not involved.
  private def isPath(ident: Identifier): Boolean =
    core.isPathIdentifier(ident) && ident.namespace.head.equalsIgnoreCase("delta")

  // With runSQLOnFiles off, Delta takes ``delta.`<location>` `` for a catalog identifier and registers it through
  // spark_catalog. Checked before every Delta create or stage.
  private def requireSqlOnFiles(): Unit =
    if (!SparkSession.active.conf.get("spark.sql.runSQLOnFiles", "true").toBoolean) {
      throw new IllegalStateException("gluecat: Delta tables need spark.sql.runSQLOnFiles=true (default); it is false")
    }

  private def onPath[T](f: => T): T = { requireSqlOnFiles(); f }

  // The setting is a runtime conf: GlueCatalog.initialize checks it, and so does every Delta operation by name.
  private def requireHookOff(ident: Identifier): Unit =
    if (!isPath(ident)) DeltaFormatSupport.requireUpdateCatalogHookOff()

  private def qualified(ident: Identifier): String = {
    val n = core.normalize(ident)
    (catalogName +: n.namespace.toSeq :+ n.name).mkString(".")
  }

  // ---- load ----

  // Every write by name loads the table first.
  override def loadTable(ident: Identifier): Table = { requireHookOff(ident); delta.loadTable(ident) }
  override def loadTable(ident: Identifier, version: String): Table = { requireHookOff(ident); delta.loadTable(ident, version) }
  override def loadTable(ident: Identifier, timestamp: Long): Table = { requireHookOff(ident); delta.loadTable(ident, timestamp) }

  // ---- create and replace ----

  // The keys that place the table go into the path identifier instead; with them Delta would reject the path as
  // ambiguous. Other option.* keys pass through as they would to Delta through spark_catalog.
  private def deltaProps(properties: java.util.Map[String, String]): java.util.Map[String, String] =
    new java.util.HashMap[String, String](
      (properties.asScala.toMap -- Seq("location", "external", "is_managed_location", "option.path")).asJava)

  // Everything that can refuse the create is checked before Delta writes anything.
  private def prepareCreate(ident: Identifier, spec: TableSpec): String = {
    spec.properties.keys.foreach(core.rejectOwnedKey)
    core.loadNamespaceMetadata(ident.namespace) // a missing database fails here, not after the Delta commit
    if (core.tableExists(ident)) {
      throw CatalogErrors.translate(catalogName, options.glueId, new GlueAlreadyExists(""), Some(core.normalize(ident)), None)
    }
    core.resolveLocation(ident, spec)
  }

  // The location of an existing Delta entry. Like Delta, refuses to replace any other table, and to move one.
  private def existingLocation(ident: Identifier, spec: TableSpec): Option[String] = core.loadEntry(ident).map { entry =>
    if (!isDeltaEntry(entry)) throw new UnsupportedOperationException(s"${qualified(ident)} is not a Delta table")
    val location = EntryConverter.location(entry)
      .getOrElse(throw new MalformedEntryException(qualified(ident), "no table location"))
    if (spec.location.exists(l => !DeltaFormatSupport.sameLocation(l, location))) {
      throw new IllegalArgumentException(
        s"${qualified(ident)} is at $location; REPLACE cannot move it to ${spec.location.get}")
    }
    spec.properties.keys.foreach(core.rejectOwnedKey)
    location
  }

  private def deltaSpec(properties: java.util.Map[String, String]): TableSpec = {
    val spec = TableProperties.split(properties, GlueCoreCatalog.defaultProvider())
    if (!isDelta(spec.provider)) {
      throw new UnsupportedOperationException("CREATE TABLE AS SELECT and REPLACE TABLE are supported for Delta tables only")
    }
    requireSqlOnFiles()
    spec
  }

  /** Writes the Glue entry for the Delta table Delta just committed at `location`. */
  private def writeGlue(ident: Identifier, location: String, spec: TableSpec, replace: Boolean)(pathTable: => Table): Unit =
    try {
      val t = pathTable
      val n = core.normalize(ident)
      val table = CatalogTable(
        identifier = TableIdentifier(n.name, n.namespace.headOption, Some(catalogName)),
        tableType = CatalogTableType.EXTERNAL,
        storage = CatalogStorageFormat.empty.copy(locationUri = Some(CatalogUtils.stringToURI(location))),
        schema = t.schema(),
        provider = Some("delta"),
        partitionColumnNames = TableProperties.partitionColumns(t.partitioning()),
        owner = spec.owner.getOrElse(""),
        properties = spec.properties,
        comment = spec.comment)
      if (replace) core.replaceTable(n, table) else core.registerTable(n, table)
    } catch {
      case NonFatal(e) if replace => throw new RuntimeException(
        s"${qualified(ident)}: Delta replaced the table at $location, but the Glue update failed: ${e.getMessage}. " +
          "The Glue entry keeps its previous columns and properties; readers take the schema from the Delta log, " +
          s"and REFRESH TABLE ${qualified(ident)} re-syncs the columns.", e)
      case NonFatal(e) => throw new DeltaCommittedButGlueFailedException(qualified(ident), location, e)
    }

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    if (isPath(ident)) return onPath(delta.createTable(ident, schema, partitions, properties))
    requireHookOff(ident)
    val spec = TableProperties.split(properties, GlueCoreCatalog.defaultProvider())
    if (!isDelta(spec.provider)) return core.createTable(ident, schema, partitions, properties)
    requireSqlOnFiles()
    val location = prepareCreate(ident, spec)
    val pathTable = delta.createTable(atPath(location), schema, partitions, deltaProps(properties))
    writeGlue(ident, location, spec, replace = false)(pathTable)
    loadTable(ident)
  }

  private def staged(ident: Identifier, location: String, spec: TableSpec, replace: Boolean)
      (stage: Identifier => StagedTable): StagedTable = {
    val table = stage(atPath(location))
    new StagedGlueTable(table, writeInfo(_, table.properties()),
      () => writeGlue(ident, location, spec, replace)(delta.loadTable(atPath(location))))
  }

  private val legacyOptionsWarned = new AtomicBoolean(false)

  // Delta's legacy storeOptionsAsProperties writes every write option into the table's metadata, so the catalog's
  // options stay out of the write while it is on; priming still gives the write the catalog's filesystem.
  private def writeInfo(info: LogicalWriteInfo, stageProperties: java.util.Map[String, String]): LogicalWriteInfo =
    if (!SparkSession.active.conf.get(DeltaFormatSupport.LegacyStoreOptions, "false").toBoolean) {
      hadoopOptions.writeOptions(info, stageProperties)
    } else {
      if (hadoopOptions.values.nonEmpty && legacyOptionsWarned.compareAndSet(false, true)) {
        DeltaFormatSupport.log.warn(s"gluecat catalog '$catalogName': ${DeltaFormatSupport.LegacyStoreOptions} is " +
          "on, so Delta writes get the catalog's Hadoop options only through its FileSystem, not as write options")
      }
      info
    }

  override def stageCreate(ident: Identifier, schema: StructType, partitions: Array[Transform],
      properties: java.util.Map[String, String]): StagedTable = {
    if (isPath(ident)) return onPath(staging.stageCreate(ident, schema, partitions, properties))
    requireHookOff(ident)
    val spec = deltaSpec(properties)
    staged(ident, prepareCreate(ident, spec), spec, replace = false)(
      staging.stageCreate(_, schema, partitions, deltaProps(properties)))
  }

  override def stageReplace(ident: Identifier, schema: StructType, partitions: Array[Transform],
      properties: java.util.Map[String, String]): StagedTable = {
    if (isPath(ident)) return onPath(staging.stageReplace(ident, schema, partitions, properties))
    requireHookOff(ident)
    val spec = deltaSpec(properties)
    val location = existingLocation(ident, spec).getOrElse(throw core.missing(core.normalize(ident)))
    staged(ident, location, spec, replace = true)(staging.stageReplace(_, schema, partitions, deltaProps(properties)))
  }

  override def stageCreateOrReplace(ident: Identifier, schema: StructType, partitions: Array[Transform],
      properties: java.util.Map[String, String]): StagedTable = {
    if (isPath(ident)) return onPath(staging.stageCreateOrReplace(ident, schema, partitions, properties))
    requireHookOff(ident)
    val spec = deltaSpec(properties)
    val existing = existingLocation(ident, spec)
    staged(ident, existing.getOrElse(prepareCreate(ident, spec)), spec, replace = existing.isDefined)(
      staging.stageCreateOrReplace(_, schema, partitions, deltaProps(properties)))
  }

  // ---- alter and refresh ----

  override def alterTable(ident: Identifier, changes: Seq[TableChange]): Table = {
    if (isPath(ident)) return delta.alterTable(ident, changes: _*)
    requireHookOff(ident)
    val entry = core.loadEntry(ident).getOrElse(throw core.missing(core.normalize(ident)))
    if (!isDeltaEntry(entry)) return core.alterTable(ident, changes: _*)
    // Delta's SET LOCATION writes through spark_catalog, so the location goes to the Glue entry only, in one update
    // together with the columns of the log at the new location.
    val (location, rest) = changes.partition {
      case s: TableChange.SetProperty => s.property == "location"
      case _ => false
    }
    if (location.nonEmpty) {
      val target = location.head.asInstanceOf[TableChange.SetProperty].value
      val dir = new Path(target)
      if (!hadoopOptions.fileSystem(target).exists(new Path(dir, "_delta_log"))) {
        throw new IllegalArgumentException(s"${qualified(ident)}: $target holds no Delta table (no _delta_log directory)")
      }
      val moved = delta.loadTable(atPath(target))
      val current = EntryConverter.toCatalogTable(catalogName, entry)
      core.replaceTable(ident, current.copy(
        storage = current.storage.copy(locationUri = Some(CatalogUtils.stringToURI(target))),
        schema = moved.schema(), partitionColumnNames = TableProperties.partitionColumns(moved.partitioning())))
    }
    if (rest.nonEmpty) {
      delta.alterTable(ident, rest: _*)
      if (rest.exists(_.isInstanceOf[TableChange.ColumnChange])) {
        try syncSchema(ident) catch {
          case NonFatal(e) => throw new RuntimeException(
            s"${qualified(ident)}: Delta committed the ALTER TABLE, but the Glue update failed: ${e.getMessage}. " +
              s"Run REFRESH TABLE ${qualified(ident)} to re-sync the Glue entry.", e)
        }
      }
    }
    loadTable(ident)
  }

  override def invalidateTable(ident: Identifier): Unit =
    if (isPath(ident)) delta.invalidateTable(ident) else syncSchema(ident)

  /** Rewrites the Glue columns, schema and partition keys of a Delta entry from its log, when they differ. The core
    * passes the entry's VersionId and keeps parameters it does not own. */
  private def syncSchema(ident: Identifier): Unit = core.loadEntry(ident).filter(isDeltaEntry).foreach { entry =>
    val current = EntryConverter.toCatalogTable(catalogName, entry)
    val table = delta.loadTable(ident)
    val partitionColumns = TableProperties.partitionColumns(table.partitioning())
    if (table.schema() != current.schema || partitionColumns != current.partitionColumnNames) {
      core.replaceTable(ident, current.copy(schema = table.schema(), partitionColumnNames = partitionColumns))
    }
  }
}

object DeltaFormatSupport {
  private val DeltaCatalogClass = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
  private val LegacyStoreOptions = "spark.databricks.delta.legacy.storeOptionsAsProperties"
  private val log = LoggerFactory.getLogger(classOf[DeltaFormatSupport])

  // The loader Spark itself uses for catalog plugins.
  private def loader: ClassLoader =
    Option(Thread.currentThread.getContextClassLoader).getOrElse(getClass.getClassLoader)

  /** Same table location: `s3`, `s3a` and `s3n` are one scheme, a trailing slash does not matter. */
  private[format] def sameLocation(a: String, b: String): Boolean = {
    def norm(l: String) = {
      val u = new Path(l).toUri
      if (u.getScheme != null && u.getScheme.toLowerCase(java.util.Locale.ROOT).matches("s3[an]?")) {
        new java.net.URI("s3", u.getAuthority, u.getPath, null, null)
      } else u
    }
    norm(a) == norm(b)
  }

  def deltaCatalogClass(): Class[_] = Class.forName(DeltaCatalogClass, true, loader)

  /** Delta's UpdateCatalog hook writes through the session's v1 catalog (spark_catalog), so nothing may run while it is
    * enabled. */
  def requireUpdateCatalogHookOff(): Unit =
    if (SparkSession.active.conf.get("spark.databricks.delta.catalog.update.enabled", "false").toBoolean) {
      throw new IllegalStateException(
        "disable spark.databricks.delta.catalog.update.enabled; Delta's UpdateCatalog hook writes through spark_catalog")
    }

  def available: Boolean =
    try { deltaCatalogClass(); true } catch { case _: ClassNotFoundException => false }
}

/** Delta's staged table for the path, plus the Glue write once Delta has committed. Abort leaves Glue untouched. The
  * write's options come from `writeInfo` (the catalog's Hadoop options under the caller's). */
private final class StagedGlueTable(staged: StagedTable, writeInfo: LogicalWriteInfo => LogicalWriteInfo,
    afterCommit: () => Unit) extends StagedTable with SupportsWrite {
  override def name(): String = staged.name()
  override def schema(): StructType = staged.schema()
  override def partitioning(): Array[Transform] = staged.partitioning()
  override def properties(): java.util.Map[String, String] = staged.properties()
  override def capabilities(): java.util.Set[TableCapability] = staged.capabilities()
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    staged.asInstanceOf[SupportsWrite].newWriteBuilder(writeInfo(info))
  override def commitStagedChanges(): Unit = { staged.commitStagedChanges(); afterCommit() }
  override def abortStagedChanges(): Unit = staged.abortStagedChanges()
}

/** What Delta sees of the core: Delta entries carry an empty schema and no partition columns, as in Delta's own
  * metastore entries, so Delta reads them from its log. Spark 3.5 passes a non-empty catalog schema to Delta's
  * streaming source, which Delta 3.3 rejects. Everything else forwards to the core. */
private final class SchemalessDeltaView(core: GlueCoreCatalog) extends TableCatalog with SupportsNamespaces {
  override def name(): String = core.name()
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = ()
  override def defaultNamespace(): Array[String] = core.defaultNamespace()

  private def view(table: Table): Table = V1TableBridge.unwrap(table) match {
    case Some(t) if t.provider.exists(_.equalsIgnoreCase("delta")) =>
      V1TableBridge.wrap(t.copy(schema = new StructType(), partitionColumnNames = Nil))
    case _ => table
  }

  override def loadTable(ident: Identifier): Table = view(core.loadTable(ident))
  override def loadTable(ident: Identifier, version: String): Table = view(core.loadTable(ident, version))
  override def loadTable(ident: Identifier, timestamp: Long): Table = view(core.loadTable(ident, timestamp))
  override def tableExists(ident: Identifier): Boolean = core.tableExists(ident)
  override def listTables(namespace: Array[String]): Array[Identifier] = core.listTables(namespace)
  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform],
      properties: java.util.Map[String, String]): Table = view(core.createTable(ident, schema, partitions, properties))
  override def alterTable(ident: Identifier, changes: TableChange*): Table = view(core.alterTable(ident, changes: _*))
  override def dropTable(ident: Identifier): Boolean = core.dropTable(ident)
  override def purgeTable(ident: Identifier): Boolean = core.purgeTable(ident)
  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = core.renameTable(oldIdent, newIdent)
  override def invalidateTable(ident: Identifier): Unit = core.invalidateTable(ident)

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
}
