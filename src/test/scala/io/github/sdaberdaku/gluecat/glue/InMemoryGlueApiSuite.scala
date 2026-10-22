package io.github.sdaberdaku.gluecat.glue

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class InMemoryGlueApiSuite extends AnyFunSuite {
  private val db = GlueDatabaseEntry("db", None, None, Map.empty)
  private val t = GlueTableEntry("db", "t", None, None, None, Map.empty,
    GlueStorage(None, None, None, None, Map.empty, Seq.empty), Seq.empty, None)

  private def withTable(): InMemoryGlueApi = {
    val api = new InMemoryGlueApi
    api.createDatabase(db)
    api.createTable(t)
    api
  }

  test("createTable sets versionId \"1\" and updateTable increments it") {
    val api = withTable()
    assert(api.getTable("db", "t").flatMap(_.versionId) == Some("1"))
    api.updateTable(t, Some("1"), skipArchive = true)
    assert(api.getTable("db", "t").flatMap(_.versionId) == Some("2"))
  }

  test("updateTable with a stale versionId fails") {
    val api = withTable()
    api.updateTable(t, Some("1"), skipArchive = true)
    intercept[GlueConcurrentModification](api.updateTable(t, Some("1"), skipArchive = true))
  }

  test("updateTable without expectedVersionId always succeeds") {
    val api = withTable()
    api.updateTable(t, None, skipArchive = true)
    api.updateTable(t, None, skipArchive = true)
    assert(api.getTable("db", "t").flatMap(_.versionId) == Some("3"))
  }

  test("createTable twice fails") {
    val api = withTable()
    intercept[GlueAlreadyExists](api.createTable(t))
  }

  test("createTable in a missing database fails") {
    intercept[GlueEntityNotFound](new InMemoryGlueApi().createTable(t))
  }

  test("deleteDatabase removes its tables") {
    val api = withTable()
    api.deleteDatabase("db")
    assert(api.getTable("db", "t").isEmpty)
    assert(api.getDatabase("db").isEmpty)
  }

  test("listTables of a missing database fails") {
    intercept[GlueEntityNotFound](new InMemoryGlueApi().listTables("db"))
  }

  test("failNext throws once") {
    val api = new InMemoryGlueApi
    api.createDatabase(db)
    api.failNext = Some(("CreateTable", new GlueFailure("CreateTable", "boom")))
    intercept[GlueFailure](api.createTable(t))
    api.createTable(t)
    assert(api.getTable("db", "t").isDefined)
  }
}
