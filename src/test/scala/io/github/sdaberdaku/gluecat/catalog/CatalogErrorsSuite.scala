package io.github.sdaberdaku.gluecat.catalog

import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.catalog.Identifier
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import org.scalatest.funsuite.AnyFunSuite

import io.github.sdaberdaku.gluecat.glue._

@RunWith(classOf[JUnitRunner])
class CatalogErrorsSuite extends AnyFunSuite {
  private val id = Some(Identifier.of(Array("db"), "t"))
  private val glueId = Some("111111111111")
  private def tr(e: GlueApiException, ident: Option[Identifier] = id, ns: Option[Array[String]] = None,
      v: Option[String] = None) = CatalogErrors.translate("acct_a", glueId, e, ident, ns, v)

  test("not found maps to NoSuchTable with catalog, database and table") {
    val r = tr(new GlueEntityNotFound("x"))
    assert(r.isInstanceOf[NoSuchTableException])
    Seq("acct_a", "db", "t").foreach(s => assert(r.getMessage.contains(s)))
  }

  test("not found without identifier maps to NoSuchNamespace") {
    assert(tr(new GlueEntityNotFound("x"), None, Some(Array("db"))).isInstanceOf[NoSuchNamespaceException])
  }

  test("already exists maps to table or namespace variant") {
    assert(tr(new GlueAlreadyExists("x")).isInstanceOf[TableAlreadyExistsException])
    assert(tr(new GlueAlreadyExists("x"), None, Some(Array("db"))).isInstanceOf[NamespaceAlreadyExistsException])
  }

  test("concurrent modification names the catalog, table and expected version") {
    val r = tr(new GlueConcurrentModification("glue says no"), v = Some("7"))
    assert(r.isInstanceOf[GlueCatalogConcurrentModificationException])
    Seq("acct_a", "db.t", "expected VersionId 7", "glue says no").foreach(s => assert(r.getMessage.contains(s)))
    assert(!tr(new GlueConcurrentModification("m")).getMessage.contains("expected VersionId"))
  }

  test("access denied names catalog and glue id") {
    val r = tr(new GlueAccessDenied("nope"))
    assert(r.isInstanceOf[RuntimeException])
    assert(r.getMessage.contains("acct_a") && r.getMessage.contains("111111111111"))
  }

  test("other failures name the operation") {
    assert(tr(new GlueFailure("GetTable", "boom")).getMessage.contains("GetTable"))
  }
}
