package io.github.sdaberdaku.gluecat.catalog

import java.util.Locale

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import io.github.sdaberdaku.gluecat.GlueCatalogOptions
import io.github.sdaberdaku.gluecat.glue._

/** Glue-backed catalog core. Names are lowercased in both directions. Glue databases carry no VersionId, so
  * concurrent `alterNamespace` calls are last-writer-wins. */
class GlueCoreCatalog(val catalogName: String, glue: GlueApi, options: GlueCatalogOptions,
    schemaThreshold: () => Int) extends TableCatalog with SupportsNamespaces {

  override def name(): String = catalogName
  // The core is constructed by its owner, never initialised by Spark.
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = ()

  override def defaultNamespace(): Array[String] = Array(options.defaultNamespace.toLowerCase(Locale.ROOT))
  def normalize(ident: Identifier): Identifier = Identifier.of(normalize(ident.namespace), lower(ident.name))
  def normalize(namespace: Array[String]): Array[String] = namespace.map(lower)
  private def lower(s: String) = s.toLowerCase(Locale.ROOT)

  private def guardNamespace[T](ns: Array[String])(f: => T): T =
    try f catch {
      case e: GlueApiException =>
        throw CatalogErrors.translate(catalogName, options.glueId, e, None, Some(normalize(ns)))
    }

  private def single(ns: Array[String]): String = {
    if (ns.length != 1) throw new UnsupportedOperationException(
      s"Catalog $catalogName supports only single-level namespaces, got '${ns.mkString(".")}'")
    lower(ns.head)
  }

  // ---- SupportsNamespaces ----

  override def listNamespaces(): Array[Array[String]] =
    guardNamespace(Array.empty[String])(glue.listDatabases().map(d => Array(lower(d.name))).toArray)

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] =
    if (namespace.isEmpty) listNamespaces() else Array.empty

  override def namespaceExists(namespace: Array[String]): Boolean =
    namespace.length == 1 && guardNamespace(namespace)(glue.getDatabase(lower(namespace.head)).isDefined)

  override def loadNamespaceMetadata(namespace: Array[String]): java.util.Map[String, String] =
    guardNamespace(namespace) {
      val db = if (namespace.length == 1) glue.getDatabase(lower(namespace.head)) else None
      EntryConverter.toNamespaceMetadata(db.getOrElse(throw new GlueEntityNotFound("")))
    }

  override def createNamespace(namespace: Array[String], metadata: java.util.Map[String, String]): Unit = {
    val db = single(namespace)
    guardNamespace(namespace)(glue.createDatabase(EntryConverter.toGlueDatabase(db, metadata)))
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    val db = single(namespace)
    guardNamespace(namespace) {
      val current = glue.getDatabase(db).getOrElse(throw new GlueEntityNotFound(s"database $db not found"))
      val updated = changes.foldLeft(current) {
        case (d, s: NamespaceChange.SetProperty) => s.property match {
          case "comment" => d.copy(description = Some(s.value))
          case "location" => d.copy(locationUri = Some(s.value))
          case k => d.copy(parameters = d.parameters + (k -> s.value))
        }
        case (d, r: NamespaceChange.RemoveProperty) => r.property match {
          case "comment" => d.copy(description = None)
          case "location" => d.copy(locationUri = None)
          case k => d.copy(parameters = d.parameters - k)
        }
        case (_, other) => throw new UnsupportedOperationException(s"Unsupported namespace change: $other")
      }
      glue.updateDatabase(updated)
    }
  }

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    if (namespace.length != 1) return false
    val db = lower(namespace.head)
    guardNamespace(namespace) {
      if (glue.getDatabase(db).isEmpty) false
      else {
        // Glue's DeleteDatabase deletes the tables, so check first (a table created between the two calls is lost).
        if (!cascade && glue.listTables(db).nonEmpty) throw new NonEmptyNamespaceException(Array(catalogName, db))
        glue.deleteDatabase(db)
        true
      }
    }
  }

  // ---- TableCatalog ----

  private def guardTable[T](ident: Identifier, expected: Option[String] = None)(f: => T): T =
    try f catch {
      case e: GlueApiException =>
        throw CatalogErrors.translate(catalogName, options.glueId, e, Some(ident), None, expected)
    }

  private def missing(ident: Identifier): Throwable =
    CatalogErrors.translate(catalogName, options.glueId, new GlueEntityNotFound(""), Some(ident), None)

  private def qualified(ident: Identifier): String = (catalogName +: ident.namespace.toSeq :+ ident.name).mkString(".")

  def loadEntry(ident: Identifier): Option[GlueTableEntry] = {
    val n = normalize(ident)
    guardTable(n)(glue.getTable(single(n.namespace), n.name))
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = guardNamespace(namespace) {
    glue.listTables(single(namespace)).filterNot(_.tableType.contains("VIRTUAL_VIEW"))
      .map(e => Identifier.of(Array(lower(e.database)), lower(e.name))).toArray
  }

  override def tableExists(ident: Identifier): Boolean = loadEntry(ident).isDefined

  override def loadTable(ident: Identifier): Table =
    V1TableBridge.wrap(EntryConverter.toCatalogTable(catalogName, loadEntry(ident).getOrElse(throw missing(normalize(ident)))))

  def resolveLocation(ident: Identifier, spec: TableSpec): String = spec.location.getOrElse {
    val n = normalize(ident)
    val db = single(n.namespace)
    val database = guardNamespace(n.namespace)(glue.getDatabase(db).getOrElse(throw new GlueEntityNotFound("")))
    def join(base: String, rest: String) = base.stripSuffix("/") + "/" + rest
    database.locationUri.map(join(_, n.name))
      .orElse(options.warehouse.map(join(_, s"$db.db/${n.name}")))
      .getOrElse(throw new IllegalArgumentException(
        s"Cannot place table ${qualified(n)}: no location given, database $db has no location and the catalog has no warehouse option"))
  }

  private val Mapped = Set("comment", "location", "owner")

  /** Keys the catalog owns (or cannot support) cannot be set or unset by users. */
  private def rejectOwnedKey(k: String): Unit =
    if (k == "collation" || k.startsWith("spark.sql.") || EntryConverter.HiddenParameters(k) ||
      (TableProperties.ReservedKeys(k) && !Mapped(k))) {
      throw new UnsupportedOperationException(s"Table property '$k' is managed by the catalog and cannot be set or removed")
    }

  private def rejectView(n: Identifier, entry: GlueTableEntry): Unit =
    if (entry.tableType.contains("VIRTUAL_VIEW")) throw new UnsupportedTableException(qualified(n), "is a view; views are not supported")

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    val n = normalize(ident)
    val spec = TableProperties.split(properties, "parquet")
    spec.properties.keys.foreach(rejectOwnedKey)
    val location = resolveLocation(n, spec)
    registerTable(n, CatalogTable(
      identifier = TableIdentifier(n.name, Some(single(n.namespace)), Some(catalogName)),
      tableType = CatalogTableType.EXTERNAL,
      storage = CatalogStorageFormat.empty.copy(locationUri = Some(CatalogUtils.stringToURI(location)),
        properties = spec.options),
      schema = schema,
      provider = Some(spec.provider),
      partitionColumnNames = TableProperties.partitionColumns(partitions),
      owner = spec.owner.getOrElse(""),
      createVersion = SPARK_VERSION,
      properties = spec.properties,
      comment = spec.comment))
    loadTable(n)
  }

  def registerTable(ident: Identifier, table: CatalogTable): Unit =
    createEntry(normalize(ident), named(normalize(ident), EntryConverter.toGlueTable(table, schemaThreshold())))

  // The entry is named by the identifier the caller targets, never by the table's own identifier.
  private def named(n: Identifier, entry: GlueTableEntry) = entry.copy(database = single(n.namespace), name = n.name)

  private def createEntry(n: Identifier, entry: GlueTableEntry): Unit =
    try glue.createTable(entry) catch {
      case e: GlueEntityNotFound =>
        throw CatalogErrors.translate(catalogName, options.glueId, e, None, Some(n.namespace))
      case e: GlueApiException =>
        throw CatalogErrors.translate(catalogName, options.glueId, e, Some(n), None)
    }

  // Unknown parameters (written by other engines) survive; the stored schema keys are rewritten as a set.
  private def mergeParameters(old: Map[String, String], updated: Map[String, String], removed: Set[String]) =
    old.filterNot { case (k, _) => EntryConverter.isSchemaKey(k) } ++ updated -- removed

  private def update(n: Identifier, entry: GlueTableEntry, newEntry: GlueTableEntry, removed: Set[String]): Unit = {
    val merged = named(n, newEntry).copy(parameters = mergeParameters(entry.parameters, newEntry.parameters, removed))
    guardTable(n, entry.versionId)(glue.updateTable(merged, entry.versionId, options.skipArchive))
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    val n = normalize(ident)
    val entry = loadEntry(n).getOrElse(throw missing(n))
    rejectView(n, entry)
    val (table, removed) = applyPropertyChanges(EntryConverter.toCatalogTable(catalogName, entry), changes)
    val columnChanges = changes.filter {
      case _: TableChange.SetProperty | _: TableChange.RemoveProperty => false
      case _ => true
    }
    val altered = table.copy(schema = SchemaChanges(table.schema, columnChanges))
    update(n, entry, EntryConverter.toGlueTable(altered, schemaThreshold()), removed)
    loadTable(n)
  }

  private def applyPropertyChanges(table: CatalogTable, changes: Seq[TableChange]): (CatalogTable, Set[String]) = {
    def reserved(k: String) = if (k == "provider" || k == "external") {
      throw new UnsupportedOperationException(s"Table property '$k' cannot be changed")
    } else rejectOwnedKey(k)
    changes.foldLeft((table, Set.empty[String])) {
      case ((t, rm), s: TableChange.SetProperty) =>
        reserved(s.property)
        s.property match {
          case "comment" => (t.copy(comment = Some(s.value)), rm)
          case "location" => (t.copy(storage = t.storage.copy(locationUri = Some(CatalogUtils.stringToURI(s.value)))), rm)
          case "owner" => (t.copy(owner = s.value), rm)
          case k => (t.copy(properties = t.properties + (k -> s.value)), rm - k)
        }
      case ((t, rm), r: TableChange.RemoveProperty) =>
        reserved(r.property)
        r.property match {
          case "comment" => (t.copy(comment = None), rm)
          case "location" => throw new UnsupportedOperationException("The table location cannot be removed")
          case "owner" => (t.copy(owner = ""), rm)
          case k => (t.copy(properties = t.properties - k), rm + k)
        }
      case (acc, _) => acc
    }
  }

  def replaceTable(ident: Identifier, table: CatalogTable): Unit = {
    val n = normalize(ident)
    val entry = loadEntry(n).getOrElse(throw missing(n))
    rejectView(n, entry)
    update(n, entry, EntryConverter.toGlueTable(table, schemaThreshold()), Set.empty)
  }

  override def dropTable(ident: Identifier): Boolean = {
    val n = normalize(ident)
    loadEntry(n).foreach(rejectView(n, _))
    try { guardTable(n)(glue.deleteTable(single(n.namespace), n.name)); true }
    catch { case _: org.apache.spark.sql.catalyst.analysis.NoSuchTableException => false }
  }

  override def purgeTable(ident: Identifier): Boolean =
    throw new UnsupportedOperationException("PURGE is not supported: dropping a table never deletes its data")

  override def invalidateTable(ident: Identifier): Unit = ()

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    val o = normalize(oldIdent)
    // Spark passes the target's name parts as they were typed: no namespace for `RENAME TO t2`, and the catalog
    // name as the first part for `RENAME TO acct_a.db.t2`.
    val n = normalize(newIdent.namespace match {
      case Array() => Identifier.of(oldIdent.namespace, newIdent.name)
      case Array(`catalogName`, db) => Identifier.of(Array(db), newIdent.name)
      case _ => newIdent
    })
    val entry = loadEntry(o).getOrElse(throw missing(o))
    rejectView(o, entry)
    if (loadEntry(n).isDefined) guardTable(n)(throw new GlueAlreadyExists(""))
    createEntry(n, entry.copy(database = single(n.namespace), name = n.name, versionId = None))
    try glue.deleteTable(single(o.namespace), o.name) catch {
      case e: GlueApiException => throw new RuntimeException(
        s"Rename of ${qualified(o)} to ${qualified(n)} is incomplete: both tables now exist. " +
          s"Drop ${qualified(o)} to finish the rename. ${e.getMessage}", e)
    }
  }
}
