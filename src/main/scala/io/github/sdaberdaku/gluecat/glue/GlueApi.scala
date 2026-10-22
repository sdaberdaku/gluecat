package io.github.sdaberdaku.gluecat.glue

trait GlueRequestObserver { def onRequest(operation: String, catalogId: Option[String]): Unit }

object GlueRequestObserver {
  val Noop: GlueRequestObserver = new GlueRequestObserver {
    override def onRequest(operation: String, catalogId: Option[String]): Unit = ()
  }
}

/** Narrow, SDK-free view of the Glue calls the catalog needs. */
trait GlueApi extends AutoCloseable {
  def getDatabase(name: String): Option[GlueDatabaseEntry]
  def listDatabases(): Seq[GlueDatabaseEntry]
  /** @throws GlueAlreadyExists */
  def createDatabase(database: GlueDatabaseEntry): Unit
  /** @throws GlueEntityNotFound */
  def updateDatabase(database: GlueDatabaseEntry): Unit
  /** Glue also deletes the tables. @throws GlueEntityNotFound */
  def deleteDatabase(name: String): Unit
  def getTable(database: String, name: String): Option[GlueTableEntry]
  /** @throws GlueEntityNotFound when the database is missing */
  def listTables(database: String): Seq[GlueTableEntry]
  /** @throws GlueAlreadyExists @throws GlueEntityNotFound when the database is missing */
  def createTable(table: GlueTableEntry): Unit
  /** @throws GlueEntityNotFound @throws GlueConcurrentModification */
  def updateTable(table: GlueTableEntry, expectedVersionId: Option[String], skipArchive: Boolean): Unit
  /** @throws GlueEntityNotFound */
  def deleteTable(database: String, name: String): Unit
}
