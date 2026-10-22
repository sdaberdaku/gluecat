package io.github.sdaberdaku.gluecat.format

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import io.github.sdaberdaku.gluecat.GlueCatalogOptions
import io.github.sdaberdaku.gluecat.catalog._
import io.github.sdaberdaku.gluecat.glue.{GlueAlreadyExists, GlueTableEntry}

/** Delta tables. Loads and alters go through Delta's own `DeltaCatalog` wrapped around the core. Creates and
  * replaces never pass a catalog identifier to Delta (it would register them in `spark_catalog`): they run on
  * the path identifier ``delta.`<location>` ``, then the Glue entry is written. */
class DeltaFormatSupport(core: GlueCoreCatalog, catalogName: String, options: GlueCatalogOptions,
    sparkOptions: CaseInsensitiveStringMap) extends FormatSupport {

  // Only Spark types here: DeltaCatalog's class differs between Delta 3.3 and 4.x.
  private val delta: DelegatingCatalogExtension = {
    val d = DeltaFormatSupport.deltaCatalogClass().getDeclaredConstructor().newInstance()
      .asInstanceOf[DelegatingCatalogExtension]
    d.setDelegateCatalog(core)
    d.initialize(catalogName, sparkOptions)
    d
  }
  private def staging = delta.asInstanceOf[StagingTableCatalog]

  def pathIdentifier(location: String): Identifier = Identifier.of(Array("delta"), location)
  def isDelta(provider: String): Boolean = provider.equalsIgnoreCase("delta")
  private def isDeltaEntry(entry: GlueTableEntry): Boolean = entry.parameters.get("spark.sql.sources.provider").exists(isDelta)

  private def qualified(ident: Identifier): String = {
    val n = core.normalize(ident)
    (catalogName +: n.namespace.toSeq :+ n.name).mkString(".")
  }

  // ---- load ----

  override def loadTable(ident: Identifier): Table = delta.loadTable(ident)
  override def loadTable(ident: Identifier, version: String): Table = delta.loadTable(ident, version)
  override def loadTable(ident: Identifier, timestamp: Long): Table = delta.loadTable(ident, timestamp)

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
    if (spec.location.exists(l => CatalogUtils.stringToURI(l).getPath != CatalogUtils.stringToURI(location).getPath)) {
      throw new IllegalArgumentException(
        s"${qualified(ident)} is at $location; REPLACE cannot move it to ${spec.location.get}")
    }
    spec.properties.keys.foreach(core.rejectOwnedKey)
    location
  }

  private def deltaSpec(properties: java.util.Map[String, String]): TableSpec = {
    val spec = TableProperties.split(properties, "parquet")
    if (!isDelta(spec.provider)) {
      throw new UnsupportedOperationException("CREATE TABLE AS SELECT and REPLACE TABLE are supported for Delta tables only")
    }
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
    val spec = TableProperties.split(properties, "parquet")
    if (!isDelta(spec.provider)) return core.createTable(ident, schema, partitions, properties)
    val location = prepareCreate(ident, spec)
    val pathTable = delta.createTable(pathIdentifier(location), schema, partitions, deltaProps(properties))
    writeGlue(ident, location, spec, replace = false)(pathTable)
    loadTable(ident)
  }

  private def staged(ident: Identifier, location: String, spec: TableSpec, replace: Boolean)
      (stage: Identifier => StagedTable): StagedTable =
    new StagedGlueTable(stage(pathIdentifier(location)),
      () => writeGlue(ident, location, spec, replace)(delta.loadTable(pathIdentifier(location))))

  override def stageCreate(ident: Identifier, schema: StructType, partitions: Array[Transform],
      properties: java.util.Map[String, String]): StagedTable = {
    val spec = deltaSpec(properties)
    staged(ident, prepareCreate(ident, spec), spec, replace = false)(
      staging.stageCreate(_, schema, partitions, deltaProps(properties)))
  }

  override def stageReplace(ident: Identifier, schema: StructType, partitions: Array[Transform],
      properties: java.util.Map[String, String]): StagedTable = {
    val spec = deltaSpec(properties)
    val location = existingLocation(ident, spec).getOrElse(throw core.missing(core.normalize(ident)))
    staged(ident, location, spec, replace = true)(staging.stageReplace(_, schema, partitions, deltaProps(properties)))
  }

  override def stageCreateOrReplace(ident: Identifier, schema: StructType, partitions: Array[Transform],
      properties: java.util.Map[String, String]): StagedTable = {
    val spec = deltaSpec(properties)
    val existing = existingLocation(ident, spec)
    staged(ident, existing.getOrElse(prepareCreate(ident, spec)), spec, replace = existing.isDefined)(
      staging.stageCreateOrReplace(_, schema, partitions, deltaProps(properties)))
  }

  // ---- alter and refresh ----

  override def alterTable(ident: Identifier, changes: Seq[TableChange]): Table = {
    val entry = core.loadEntry(ident).getOrElse(throw core.missing(core.normalize(ident)))
    if (!isDeltaEntry(entry)) return core.alterTable(ident, changes: _*)
    // Delta's SET LOCATION writes through spark_catalog, so the location goes to the Glue entry only.
    val (location, rest) = changes.partition {
      case s: TableChange.SetProperty => s.property == "location"
      case _ => false
    }
    if (location.nonEmpty) core.alterTable(ident, location: _*)
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

  override def invalidateTable(ident: Identifier): Unit = syncSchema(ident)

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

  // The loader Spark itself uses for catalog plugins.
  private def loader: ClassLoader =
    Option(Thread.currentThread.getContextClassLoader).getOrElse(getClass.getClassLoader)

  def deltaCatalogClass(): Class[_] = Class.forName(DeltaCatalogClass, true, loader)

  def available: Boolean =
    try { deltaCatalogClass(); true } catch { case _: ClassNotFoundException => false }
}

/** Delta's staged table for the path, plus the Glue write once Delta has committed. Abort leaves Glue untouched. */
private final class StagedGlueTable(staged: StagedTable, afterCommit: () => Unit) extends StagedTable with SupportsWrite {
  override def name(): String = staged.name()
  override def schema(): StructType = staged.schema()
  override def partitioning(): Array[Transform] = staged.partitioning()
  override def properties(): java.util.Map[String, String] = staged.properties()
  override def capabilities(): java.util.Set[TableCapability] = staged.capabilities()
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    staged.asInstanceOf[SupportsWrite].newWriteBuilder(info)
  override def commitStagedChanges(): Unit = { staged.commitStagedChanges(); afterCommit() }
  override def abortStagedChanges(): Unit = staged.abortStagedChanges()
}
