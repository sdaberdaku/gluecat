package io.github.sdaberdaku.gluecat.catalog

import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException
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
        throw CatalogErrors.translate(catalogName, options.glueId, e, None, Some(ns))
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
      EntryConverter.toNamespaceMetadata(db.getOrElse(throw new GlueEntityNotFound(
        s"database ${namespace.mkString(".")} not found (glue.id=${options.glueId.getOrElse("<default>")})")))
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
        // Glue's DeleteDatabase deletes the tables, so check first.
        if (!cascade && glue.listTables(db).nonEmpty) throw new NonEmptyNamespaceException(namespace)
        glue.deleteDatabase(db)
        true
      }
    }
  }

  // ---- TableCatalog (Task 9 replaces this block) ----

  override def listTables(namespace: Array[String]): Array[Identifier] = throw new UnsupportedOperationException("Task 9")
  override def loadTable(ident: Identifier): Table = throw new UnsupportedOperationException("Task 9")
  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform],
      properties: java.util.Map[String, String]): Table = throw new UnsupportedOperationException("Task 9")
  override def alterTable(ident: Identifier, changes: TableChange*): Table = throw new UnsupportedOperationException("Task 9")
  override def dropTable(ident: Identifier): Boolean = throw new UnsupportedOperationException("Task 9")
  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = throw new UnsupportedOperationException("Task 9")
}
