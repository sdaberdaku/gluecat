package io.github.sdaberdaku.gluecat.it

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import io.github.sdaberdaku.gluecat.{GlueCatalog, GlueCatalogOptions}
import io.github.sdaberdaku.gluecat.glue._

/** GlueCatalog whose Glue layer records every request and can be told to fail the next call of an operation. */
class TestGlueCatalog extends GlueCatalog {
  override protected def newGlueApi(options: GlueCatalogOptions): GlueApi =
    new RecordingGlueApi(SdkGlueApi(options, TestGlueCatalog.Recorder(name())), name())
}

object TestGlueCatalog {
  /** (catalog name, Glue operation, CatalogId) of every request, from every catalog in the JVM. */
  val recorded = new ConcurrentLinkedQueue[(String, String, Option[String])]()

  private[it] val failures = new ConcurrentHashMap[(String, String), Throwable]()

  /** The next `operation` (e.g. "CreateTable") of catalog `catalogName` throws `exception` instead of calling Glue. */
  def failNext(catalogName: String, operation: String, exception: Throwable): Unit =
    failures.put((catalogName, operation), exception)

  /** Every suite runs in the same JVM and reuses catalog names, so each session starts from nothing. */
  def reset(): Unit = {
    recorded.clear()
    failures.clear()
  }

  final case class Recorder(catalogName: String) extends GlueRequestObserver {
    override def onRequest(operation: String, catalogId: Option[String]): Unit =
      recorded.add((catalogName, operation, catalogId))
  }
}

final class RecordingGlueApi(delegate: GlueApi, catalogName: String) extends GlueApi {
  private def call[T](operation: String)(f: => T): T = {
    val failure = TestGlueCatalog.failures.remove((catalogName, operation))
    if (failure != null) throw failure
    f
  }

  override def getDatabase(name: String): Option[GlueDatabaseEntry] = call("GetDatabase")(delegate.getDatabase(name))
  override def listDatabases(): Seq[GlueDatabaseEntry] = call("GetDatabases")(delegate.listDatabases())
  override def createDatabase(database: GlueDatabaseEntry): Unit = call("CreateDatabase")(delegate.createDatabase(database))
  override def updateDatabase(database: GlueDatabaseEntry): Unit = call("UpdateDatabase")(delegate.updateDatabase(database))
  override def deleteDatabase(name: String): Unit = call("DeleteDatabase")(delegate.deleteDatabase(name))
  override def getTable(database: String, name: String): Option[GlueTableEntry] =
    call("GetTable")(delegate.getTable(database, name))
  override def listTables(database: String): Seq[GlueTableEntry] = call("GetTables")(delegate.listTables(database))
  override def createTable(table: GlueTableEntry): Unit = call("CreateTable")(delegate.createTable(table))
  override def updateTable(table: GlueTableEntry, expectedVersionId: Option[String], skipArchive: Boolean): Unit =
    call("UpdateTable")(delegate.updateTable(table, expectedVersionId, skipArchive))
  override def deleteTable(database: String, name: String): Unit = call("DeleteTable")(delegate.deleteTable(database, name))
  override def close(): Unit = delegate.close()
}
