package io.github.sdaberdaku.gluecat.glue

import scala.collection.mutable

/** Test support: Glue in a map. Table versionId is "1" on create and increments on update. */
class InMemoryGlueApi extends GlueApi {
  private val databases = mutable.LinkedHashMap.empty[String, GlueDatabaseEntry]
  private val tables = mutable.LinkedHashMap.empty[(String, String), GlueTableEntry]

  var failNext: Option[(String, GlueApiException)] = None
  val calls: mutable.Buffer[String] = mutable.ArrayBuffer.empty[String]

  private def enter(op: String): Unit = {
    calls += op
    failNext match {
      case Some((`op`, e)) => failNext = None; throw e
      case _ =>
    }
  }

  override def getDatabase(name: String): Option[GlueDatabaseEntry] = { enter("GetDatabase"); databases.get(name) }
  override def listDatabases(): Seq[GlueDatabaseEntry] = { enter("GetDatabases"); databases.values.toList }

  override def createDatabase(database: GlueDatabaseEntry): Unit = {
    enter("CreateDatabase")
    if (databases.contains(database.name)) throw new GlueAlreadyExists(s"database ${database.name} exists")
    databases(database.name) = database
  }

  override def updateDatabase(database: GlueDatabaseEntry): Unit = {
    enter("UpdateDatabase")
    if (!databases.contains(database.name)) throw new GlueEntityNotFound(s"database ${database.name} not found")
    databases(database.name) = database
  }

  override def deleteDatabase(name: String): Unit = {
    enter("DeleteDatabase")
    if (databases.remove(name).isEmpty) throw new GlueEntityNotFound(s"database $name not found")
    tables.keys.filter(_._1 == name).toList.foreach(tables.remove)
  }

  override def getTable(database: String, name: String): Option[GlueTableEntry] = {
    enter("GetTable"); tables.get((database, name))
  }

  override def listTables(database: String): Seq[GlueTableEntry] = {
    enter("GetTables")
    if (!databases.contains(database)) throw new GlueEntityNotFound(s"database $database not found")
    tables.values.filter(_.database == database).toList
  }

  override def createTable(table: GlueTableEntry): Unit = {
    enter("CreateTable")
    if (!databases.contains(table.database)) throw new GlueEntityNotFound(s"database ${table.database} not found")
    if (tables.contains((table.database, table.name))) throw new GlueAlreadyExists(s"table ${table.name} exists")
    tables((table.database, table.name)) = table.copy(versionId = Some("1"))
  }

  override def updateTable(table: GlueTableEntry, expectedVersionId: Option[String], skipArchive: Boolean): Unit = {
    enter("UpdateTable")
    val current = tables.getOrElse((table.database, table.name),
      throw new GlueEntityNotFound(s"table ${table.name} not found"))
    if (expectedVersionId.isDefined && expectedVersionId != current.versionId) {
      throw new GlueConcurrentModification(s"table ${table.name} changed")
    }
    val next = current.versionId.map(_.toLong).getOrElse(0L) + 1
    tables((table.database, table.name)) = table.copy(versionId = Some(next.toString))
  }

  override def deleteTable(database: String, name: String): Unit = {
    enter("DeleteTable")
    if (tables.remove((database, name)).isEmpty) throw new GlueEntityNotFound(s"table $name not found")
  }

  override def close(): Unit = ()
}
