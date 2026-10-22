package io.github.sdaberdaku.gluecat.glue

import scala.collection.JavaConverters._

import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.glue.GlueClient
import software.amazon.awssdk.services.glue.model.{Option => _, _}

import io.github.sdaberdaku.gluecat.GlueCatalogOptions

class SdkGlueApi(client: GlueClient, catalogId: Option[String], observer: GlueRequestObserver) extends GlueApi {
  private val PageSize = 100

  /** Reports the call, runs it and maps SDK failures to the SDK-free exceptions. */
  private def guard[T](operation: String)(f: => T): T = {
    observer.onRequest(operation, catalogId)
    val where = s"Glue $operation" + catalogId.map(id => s" (catalog $id)").getOrElse("")
    try f
    catch {
      case e: EntityNotFoundException => throw new GlueEntityNotFound(s"$where: ${e.getMessage}", e)
      case e: AlreadyExistsException => throw new GlueAlreadyExists(s"$where: ${e.getMessage}", e)
      case e: ConcurrentModificationException => throw new GlueConcurrentModification(s"$where: ${e.getMessage}", e)
      case e: AccessDeniedException => throw new GlueAccessDenied(s"$where: ${e.getMessage}", e)
      case e: GlueException => throw new GlueFailure(operation, s"$where failed: ${e.getMessage}", e)
      case e: SdkClientException => throw new GlueFailure(operation, s"$where failed: ${e.getMessage}", e)
    }
  }

  private def notFoundAsNone[T](f: => T): Option[T] =
    try Some(f) catch { case _: GlueEntityNotFound => None }

  override def getDatabase(name: String): Option[GlueDatabaseEntry] = notFoundAsNone {
    guard("GetDatabase") {
      val b = GetDatabaseRequest.builder().name(name)
      catalogId.foreach(b.catalogId)
      toEntry(client.getDatabase(b.build()).database())
    }
  }

  override def listDatabases(): Seq[GlueDatabaseEntry] = {
    val out = Seq.newBuilder[GlueDatabaseEntry]
    var token: String = null
    do {
      val resp = guard("GetDatabases") {
        val b = GetDatabasesRequest.builder().maxResults(PageSize)
        catalogId.foreach(b.catalogId)
        if (token != null) b.nextToken(token)
        client.getDatabases(b.build())
      }
      resp.databaseList().asScala.foreach(d => out += toEntry(d))
      token = resp.nextToken()
    } while (token != null)
    out.result()
  }

  override def createDatabase(database: GlueDatabaseEntry): Unit = guard("CreateDatabase") {
    val b = CreateDatabaseRequest.builder().databaseInput(toInput(database))
    catalogId.foreach(b.catalogId)
    client.createDatabase(b.build())
  }

  override def updateDatabase(database: GlueDatabaseEntry): Unit = guard("UpdateDatabase") {
    val b = UpdateDatabaseRequest.builder().name(database.name).databaseInput(toInput(database))
    catalogId.foreach(b.catalogId)
    client.updateDatabase(b.build())
  }

  override def deleteDatabase(name: String): Unit = guard("DeleteDatabase") {
    val b = DeleteDatabaseRequest.builder().name(name)
    catalogId.foreach(b.catalogId)
    client.deleteDatabase(b.build())
  }

  override def getTable(database: String, name: String): Option[GlueTableEntry] = notFoundAsNone {
    guard("GetTable") {
      val b = GetTableRequest.builder().databaseName(database).name(name)
      catalogId.foreach(b.catalogId)
      toEntry(client.getTable(b.build()).table())
    }
  }

  override def listTables(database: String): Seq[GlueTableEntry] = {
    val out = Seq.newBuilder[GlueTableEntry]
    var token: String = null
    do {
      val resp = guard("GetTables") {
        val b = GetTablesRequest.builder().databaseName(database).maxResults(PageSize)
        catalogId.foreach(b.catalogId)
        if (token != null) b.nextToken(token)
        client.getTables(b.build())
      }
      resp.tableList().asScala.foreach(t => out += toEntry(t))
      token = resp.nextToken()
    } while (token != null)
    out.result()
  }

  override def createTable(table: GlueTableEntry): Unit = guard("CreateTable") {
    val b = CreateTableRequest.builder().databaseName(table.database).tableInput(toInput(table))
    catalogId.foreach(b.catalogId)
    client.createTable(b.build())
  }

  override def updateTable(table: GlueTableEntry, expectedVersionId: Option[String], skipArchive: Boolean): Unit =
    guard("UpdateTable") {
      val b = UpdateTableRequest.builder().databaseName(table.database).tableInput(toInput(table))
        .skipArchive(skipArchive)
      catalogId.foreach(b.catalogId)
      expectedVersionId.foreach(b.versionId)
      client.updateTable(b.build())
    }

  override def deleteTable(database: String, name: String): Unit = guard("DeleteTable") {
    val b = DeleteTableRequest.builder().databaseName(database).name(name)
    catalogId.foreach(b.catalogId)
    client.deleteTable(b.build())
  }

  override def close(): Unit = client.close()

  private def toInput(d: GlueDatabaseEntry): DatabaseInput = {
    val b = DatabaseInput.builder().name(d.name).parameters(d.parameters.asJava)
    d.description.foreach(b.description)
    d.locationUri.foreach(b.locationUri)
    b.build()
  }

  private def toEntry(d: Database): GlueDatabaseEntry =
    GlueDatabaseEntry(d.name, Option(d.description), Option(d.locationUri), params(d.hasParameters, d.parameters))

  private def toInput(t: GlueTableEntry): TableInput = {
    val s = t.storage
    val serde = SerDeInfo.builder().parameters(s.serdeParameters.asJava)
    s.serdeLibrary.foreach(serde.serializationLibrary)
    val sd = StorageDescriptor.builder().serdeInfo(serde.build()).columns(s.columns.map(toColumn).asJava)
    s.location.foreach(sd.location)
    s.inputFormat.foreach(sd.inputFormat)
    s.outputFormat.foreach(sd.outputFormat)
    val b = TableInput.builder().name(t.name).parameters(t.parameters.asJava).storageDescriptor(sd.build())
      .partitionKeys(t.partitionKeys.map(toColumn).asJava)
    t.tableType.foreach(b.tableType)
    t.description.foreach(b.description)
    t.owner.foreach(b.owner)
    b.build()
  }

  private def toColumn(c: GlueColumn): Column = {
    val b = Column.builder().name(c.name).`type`(c.dataType)
    c.comment.foreach(b.comment)
    b.build()
  }

  private def toEntry(t: Table): GlueTableEntry = {
    val sd = Option(t.storageDescriptor)
    val serde = sd.flatMap(x => Option(x.serdeInfo))
    val storage = GlueStorage(
      location = sd.flatMap(x => Option(x.location)),
      inputFormat = sd.flatMap(x => Option(x.inputFormat)),
      outputFormat = sd.flatMap(x => Option(x.outputFormat)),
      serdeLibrary = serde.flatMap(x => Option(x.serializationLibrary)),
      serdeParameters = serde.map(x => params(x.hasParameters, x.parameters)).getOrElse(Map.empty),
      columns = sd.map(x => columns(x.hasColumns, x.columns)).getOrElse(Seq.empty))
    GlueTableEntry(t.databaseName, t.name, Option(t.tableType), Option(t.description), Option(t.owner),
      params(t.hasParameters, t.parameters), storage, columns(t.hasPartitionKeys, t.partitionKeys),
      Option(t.versionId))
  }

  private def fromColumn(c: Column): GlueColumn = GlueColumn(c.name, c.`type`, Option(c.comment))

  private def columns(has: Boolean, cs: java.util.List[Column]): Seq[GlueColumn] =
    if (has) cs.asScala.map(fromColumn).toList else Seq.empty

  private def params(has: Boolean, m: java.util.Map[String, String]): Map[String, String] =
    if (has) m.asScala.toMap else Map.empty
}

object SdkGlueApi {
  def apply(options: GlueCatalogOptions, observer: GlueRequestObserver = GlueRequestObserver.Noop): SdkGlueApi =
    new SdkGlueApi(GlueClients.build(options), options.glueId, observer)
}
