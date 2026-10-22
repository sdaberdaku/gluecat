package io.github.sdaberdaku.gluecat.catalog

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchNamespaceException, NonEmptyNamespaceException}
import org.apache.spark.sql.connector.catalog.NamespaceChange
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import org.scalatest.funsuite.AnyFunSuite

import io.github.sdaberdaku.gluecat.GlueCatalogOptions
import io.github.sdaberdaku.gluecat.glue._

@RunWith(classOf[JUnitRunner])
class GlueCoreCatalogNamespaceSuite extends AnyFunSuite {
  private def opts(extra: (String, String)*) =
    GlueCatalogOptions.parse("acct_a", (Map("glue.id" -> "111111111111") ++ extra).asJava)
  private def fixture(extra: (String, String)*) = {
    val glue = new InMemoryGlueApi
    (new GlueCoreCatalog("acct_a", glue, opts(extra: _*), () => 4000), glue)
  }
  private def md(kv: (String, String)*) = kv.toMap.asJava
  private val empty = md()

  test("createNamespace then loadNamespaceMetadata") {
    val (c, _) = fixture()
    c.createNamespace(Array("db"), md("comment" -> "c", "location" -> "s3://bucket-a/db", "team" -> "x"))
    val m = c.loadNamespaceMetadata(Array("db")).asScala
    assert(m("comment") == "c" && m("location") == "s3://bucket-a/db" && m("team") == "x")
  }

  test("createNamespace without a location uses <warehouse>/<db>.db, or none without a warehouse") {
    val (withWarehouse, glueA) = fixture("warehouse" -> "s3://bucket-a/wh/")
    withWarehouse.createNamespace(Array("Db"), empty)
    withWarehouse.createNamespace(Array("given"), md("location" -> "s3://bucket-a/given"))
    assert(glueA.getDatabase("db").flatMap(_.locationUri).contains("s3://bucket-a/wh/db.db"))
    assert(glueA.getDatabase("given").flatMap(_.locationUri).contains("s3://bucket-a/given"))
    val (without, glueB) = fixture()
    without.createNamespace(Array("db"), empty)
    assert(glueB.getDatabase("db").exists(_.locationUri.isEmpty))
  }

  test("names are lowercased") {
    val (c, _) = fixture()
    c.createNamespace(Array("MyDb"), empty)
    assert(c.namespaceExists(Array("mydb")) && c.namespaceExists(Array("MYDB")))
    assert(c.listNamespaces().toSeq.map(_.toSeq) == Seq(Seq("mydb")))
  }

  test("listNamespaces(nonEmpty) is empty") {
    val (c, _) = fixture()
    c.createNamespace(Array("db"), empty)
    assert(c.listNamespaces(Array("db")).isEmpty)
  }

  test("multi-level create fails") {
    val (c, _) = fixture()
    val e = intercept[UnsupportedOperationException](c.createNamespace(Array("a", "b"), empty))
    assert(e.getMessage.contains("single-level"))
  }

  test("create twice") {
    val (c, _) = fixture()
    c.createNamespace(Array("db"), empty)
    intercept[NamespaceAlreadyExistsException](c.createNamespace(Array("db"), empty))
  }

  test("load missing") {
    val (c, _) = fixture()
    val e = intercept[NoSuchNamespaceException](c.loadNamespaceMetadata(Array("db")))
    assert(e.getMessage.contains("acct_a") && e.getMessage.contains("111111111111"))
    assert(!c.namespaceExists(Array("db")))
  }

  test("alterNamespace") {
    val (c, _) = fixture()
    c.createNamespace(Array("db"), md("comment" -> "c", "old" -> "1"))
    c.alterNamespace(Array("db"), NamespaceChange.setProperty("comment", "d"),
      NamespaceChange.setProperty("team", "y"), NamespaceChange.removeProperty("old"))
    val m = c.loadNamespaceMetadata(Array("db")).asScala
    assert(m("comment") == "d" && m("team") == "y" && !m.contains("old"))
    c.alterNamespace(Array("db"), NamespaceChange.setProperty("location", "s3://bucket-a/new"))
    assert(c.loadNamespaceMetadata(Array("db")).get("location") == "s3://bucket-a/new")
    c.alterNamespace(Array("db"), NamespaceChange.removeProperty("comment"))
    assert(!c.loadNamespaceMetadata(Array("db")).containsKey("comment"))
  }

  test("dropNamespace refuses non-empty without cascade") {
    val (c, glue) = fixture()
    c.createNamespace(Array("db"), empty)
    glue.createTable(GlueTableEntry("db", "t", None, None, None, Map.empty,
      GlueStorage(None, None, None, None, Map.empty, Nil), Nil, None))
    intercept[NonEmptyNamespaceException](c.dropNamespace(Array("db"), false))
    assert(c.dropNamespace(Array("db"), true))
    assert(!c.namespaceExists(Array("db")))
    assert(glue.getTable("db", "t").isEmpty)
  }

  test("dropNamespace of missing returns false") {
    val (c, _) = fixture()
    assert(!c.dropNamespace(Array("db"), false))
    assert(!c.dropNamespace(Array("db"), true))
  }

  test("defaultNamespace") {
    assert(fixture()._1.defaultNamespace().toSeq == Seq("default"))
    assert(fixture("default-namespace" -> "main")._1.defaultNamespace().toSeq == Seq("main"))
    assert(fixture("default-namespace" -> "Main")._1.defaultNamespace().toSeq == Seq("main"))
  }

  test("Glue errors are translated") {
    val (c, glue) = fixture()
    glue.failNext = Some("GetDatabases" -> new GlueAccessDenied("nope"))
    val e = intercept[RuntimeException](c.listNamespaces())
    assert(e.getMessage.contains("acct_a") && e.getMessage.contains("access denied"))
  }
}
