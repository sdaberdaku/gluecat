package io.github.sdaberdaku.gluecat.catalog

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.catalog.Identifier

import io.github.sdaberdaku.gluecat.glue._

/** The table exists but is not one this catalog can load (Iceberg, view, no data source provider). */
final class UnsupportedTableException(val identifier: String, reason: String)
    extends AnalysisException(s"$identifier: $reason")

/** The Glue entry claims to be a Spark data source table but its contents are inconsistent. */
final class MalformedEntryException(val identifier: String, reason: String)
    extends AnalysisException(s"$identifier: $reason")

final class GlueCatalogConcurrentModificationException(message: String, cause: Throwable)
    extends RuntimeException(message, cause)

final class DeltaCommittedButGlueFailedException(val identifier: String, val location: String, cause: Throwable)
    extends RuntimeException(
      s"$identifier: Delta commit succeeded at $location, but the Glue update failed: ${cause.getMessage}. " +
        s"To register the table, run: CREATE TABLE $identifier USING delta LOCATION '$location'",
      cause)

object CatalogErrors {

  // Spark's NoSuch*/AlreadyExists* constructors that exist on both 3.5 and 4.x and carry a custom name are the
  // multi-part ones, so the catalog name is part of the name they report.
  def translate(catalogName: String, glueId: Option[String], e: GlueApiException, ident: Option[Identifier],
      namespace: Option[Array[String]]): Throwable = {
    val ns = ident.map(_.namespace.toSeq).orElse(namespace.map(_.toSeq)).getOrElse(Seq.empty)
    val where = ident.map(i => (catalogName +: ns) :+ i.name).getOrElse(catalogName +: ns)
    def id = where.mkString(".")
    def glue = glueId.getOrElse("<default>")
    e match {
      case _: GlueEntityNotFound =>
        if (ident.isDefined) new NoSuchTableException(where) else new NoSuchNamespaceException(where)
      case _: GlueAlreadyExists =>
        if (ident.isDefined) new TableAlreadyExistsException(where)
        else new NamespaceAlreadyExistsException(where.toArray)
      case _: GlueConcurrentModification =>
        new GlueCatalogConcurrentModificationException(
          s"Concurrent modification of $id in catalog $catalogName; retry the statement", e)
      case _: GlueAccessDenied =>
        new RuntimeException(s"Catalog $catalogName (glue.id=$glue): access denied: ${e.getMessage}", e)
      case f: GlueFailure =>
        new RuntimeException(s"Catalog $catalogName (glue.id=$glue): ${f.operation} failed: ${e.getMessage}", e)
    }
  }
}
