package io.github.sdaberdaku.gluecat.glue

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.glue.GlueClient
import software.amazon.awssdk.services.glue.model.{Option => _, _}

@RunWith(classOf[JUnitRunner])
class SdkGlueApiSuite extends AnyFunSuite {
  private val Cat = "111111111111"

  private class Recorder extends GlueRequestObserver {
    val calls = ArrayBuffer.empty[(String, Option[String])]
    override def onRequest(operation: String, catalogId: Option[String]): Unit = calls += operation -> catalogId
  }

  private abstract class Stub extends GlueClient {
    def serviceName(): String = "glue"
    def close(): Unit = ()
  }

  private def sdkTable(name: String): Table = Table.builder().name(name).databaseName("db").build()

  private val entry = GlueTableEntry("db", "t", Some("EXTERNAL_TABLE"), Some("desc"), Some("me"), Map("k" -> "v"),
    GlueStorage(Some("s3://bucket-a/t"), Some("in.Format"), Some("out.Format"), Some("serde.Lib"),
      Map("sk" -> "sv"), Seq(GlueColumn("c1", "string", Some("first")), GlueColumn("c2", "int"))),
    Seq(GlueColumn("p", "string")), Some("3"))

  test("listTables follows NextToken") {
    val requests = ArrayBuffer.empty[GetTablesRequest]
    val stub = new Stub {
      override def getTables(r: GetTablesRequest): GetTablesResponse = {
        requests += r
        if (r.nextToken() == null)
          GetTablesResponse.builder().tableList((1 to 100).map(i => sdkTable(s"t$i")).asJava).nextToken("p2").build()
        else GetTablesResponse.builder().tableList((101 to 105).map(i => sdkTable(s"t$i")).asJava).build()
      }
    }
    val obs = new Recorder
    assert(new SdkGlueApi(stub, Some(Cat), obs).listTables("db").size == 105)
    assert(requests.map(_.catalogId()) == Seq(Cat, Cat))
    assert(requests.map(_.maxResults().intValue) == Seq(100, 100))
    assert(requests.map(_.nextToken()) == Seq(null, "p2"))
    assert(obs.calls == Seq("GetTables" -> Some(Cat), "GetTables" -> Some(Cat)))
  }

  test("listDatabases follows NextToken") {
    val requests = ArrayBuffer.empty[GetDatabasesRequest]
    def db(i: Int) = Database.builder().name(s"d$i").build()
    val stub = new Stub {
      override def getDatabases(r: GetDatabasesRequest): GetDatabasesResponse = {
        requests += r
        if (r.nextToken() == null)
          GetDatabasesResponse.builder().databaseList((1 to 100).map(db).asJava).nextToken("p2").build()
        else GetDatabasesResponse.builder().databaseList((101 to 105).map(db).asJava).build()
      }
    }
    val obs = new Recorder
    assert(new SdkGlueApi(stub, Some(Cat), obs).listDatabases().size == 105)
    assert(requests.map(_.catalogId()) == Seq(Cat, Cat))
    assert(requests.map(_.maxResults().intValue) == Seq(100, 100))
    assert(requests.map(_.nextToken()) == Seq(null, "p2"))
    assert(obs.calls == Seq("GetDatabases" -> Some(Cat), "GetDatabases" -> Some(Cat)))
  }

  private class Seen extends Stub {
    val ids = ArrayBuffer.empty[String]
    override def getTable(r: GetTableRequest): GetTableResponse = { ids += r.catalogId(); GetTableResponse.builder().table(sdkTable("t")).build() }
    override def createTable(r: CreateTableRequest): CreateTableResponse = { ids += r.catalogId(); CreateTableResponse.builder().build() }
    override def updateTable(r: UpdateTableRequest): UpdateTableResponse = { ids += r.catalogId(); UpdateTableResponse.builder().build() }
    override def deleteTable(r: DeleteTableRequest): DeleteTableResponse = { ids += r.catalogId(); DeleteTableResponse.builder().build() }
    override def getDatabase(r: GetDatabaseRequest): GetDatabaseResponse = { ids += r.catalogId(); GetDatabaseResponse.builder().database(Database.builder().name("db").build()).build() }
    override def createDatabase(r: CreateDatabaseRequest): CreateDatabaseResponse = { ids += r.catalogId(); CreateDatabaseResponse.builder().build() }
    override def updateDatabase(r: UpdateDatabaseRequest): UpdateDatabaseResponse = { ids += r.catalogId(); UpdateDatabaseResponse.builder().build() }
    override def deleteDatabase(r: DeleteDatabaseRequest): DeleteDatabaseResponse = { ids += r.catalogId(); DeleteDatabaseResponse.builder().build() }
  }

  private def runAll(api: SdkGlueApi): Unit = {
    api.getTable("db", "t"); api.createTable(entry); api.updateTable(entry, Some("3"), skipArchive = true)
    api.deleteTable("db", "t"); api.getDatabase("db")
    api.createDatabase(GlueDatabaseEntry("db", None, None, Map.empty))
    api.updateDatabase(GlueDatabaseEntry("db", None, None, Map.empty)); api.deleteDatabase("db")
  }

  private val ops = Seq("GetTable", "CreateTable", "UpdateTable", "DeleteTable", "GetDatabase", "CreateDatabase",
    "UpdateDatabase", "DeleteDatabase")

  test("every request carries the configured CatalogId and reports to the observer") {
    val stub = new Seen; val obs = new Recorder
    runAll(new SdkGlueApi(stub, Some(Cat), obs))
    assert(obs.calls == ops.map(_ -> Some(Cat)))
    assert(stub.ids == ops.map(_ => Cat))
  }

  test("no glue.id means no CatalogId") {
    val stub = new Seen; val obs = new Recorder
    runAll(new SdkGlueApi(stub, None, obs))
    assert(obs.calls == ops.map(_ -> None))
    assert(stub.ids.forall(_ == null) && stub.ids.size == ops.size)
  }

  private def failing(e: => Throwable): SdkGlueApi = new SdkGlueApi(new Stub {
    override def getTable(r: GetTableRequest): GetTableResponse = throw e
    override def createTable(r: CreateTableRequest): CreateTableResponse = throw e
    override def updateTable(r: UpdateTableRequest): UpdateTableResponse = throw e
  }, Some(Cat), GlueRequestObserver.Noop)

  test("getTable maps EntityNotFoundException to None") {
    assert(failing(EntityNotFoundException.builder().message("x").build()).getTable("db", "t").isEmpty)
  }

  test("createTable maps AlreadyExistsException to GlueAlreadyExists") {
    val cause = AlreadyExistsException.builder().message("x").build()
    val e = intercept[GlueAlreadyExists](failing(cause).createTable(entry))
    assert(e.getCause eq cause)
    assert(e.getMessage.contains("CreateTable") && e.getMessage.contains(Cat))
  }

  test("updateTable maps ConcurrentModificationException to GlueConcurrentModification") {
    intercept[GlueConcurrentModification](
      failing(ConcurrentModificationException.builder().message("x").build()).updateTable(entry, Some("3"), true))
  }

  test("updateTable maps EntityNotFoundException to GlueEntityNotFound") {
    intercept[GlueEntityNotFound](
      failing(EntityNotFoundException.builder().message("x").build()).updateTable(entry, Some("3"), true))
  }

  test("AccessDeniedException maps to GlueAccessDenied") {
    intercept[GlueAccessDenied](failing(AccessDeniedException.builder().message("x").build()).getTable("db", "t"))
  }

  test("any other GlueException maps to GlueFailure with the operation name") {
    val e = intercept[GlueFailure](failing(InternalServiceException.builder().message("boom").build()).getTable("db", "t"))
    assert(e.operation == "GetTable")
    assert(e.getMessage.contains("GetTable"))
  }

  test("SdkClientException maps to GlueFailure") {
    val e = intercept[GlueFailure](failing(SdkClientException.create("no network")).createTable(entry))
    assert(e.operation == "CreateTable")
  }

  test("updateTable passes versionId and skipArchive") {
    val seen = ArrayBuffer.empty[UpdateTableRequest]
    val stub = new Stub {
      override def updateTable(r: UpdateTableRequest): UpdateTableResponse = { seen += r; UpdateTableResponse.builder().build() }
    }
    new SdkGlueApi(stub, Some(Cat), GlueRequestObserver.Noop).updateTable(entry, Some("7"), skipArchive = true)
    assert(seen.head.versionId() == "7")
    assert(seen.head.skipArchive() == true)
  }

  test("model round trip") {
    val created = ArrayBuffer.empty[CreateTableRequest]
    val stored = ArrayBuffer.empty[TableInput]
    val stub = new Stub {
      override def createTable(r: CreateTableRequest): CreateTableResponse = { created += r; CreateTableResponse.builder().build() }
      override def getTable(r: GetTableRequest): GetTableResponse = {
        val in = created.head.tableInput()
        GetTableResponse.builder().table(Table.builder().name(in.name()).databaseName(r.databaseName())
          .description(in.description()).owner(in.owner()).parameters(in.parameters()).tableType(in.tableType())
          .storageDescriptor(in.storageDescriptor()).partitionKeys(in.partitionKeys()).versionId("3").build()).build()
      }
    }
    val api = new SdkGlueApi(stub, Some(Cat), GlueRequestObserver.Noop)
    api.createTable(entry)
    val r = created.head
    assert(r.databaseName() == "db")
    val in = r.tableInput()
    assert(in.name() == "t" && in.description() == "desc" && in.owner() == "me" && in.tableType() == "EXTERNAL_TABLE")
    assert(in.parameters().asScala == Map("k" -> "v"))
    val sd = in.storageDescriptor()
    assert(sd.location() == "s3://bucket-a/t" && sd.inputFormat() == "in.Format" && sd.outputFormat() == "out.Format")
    assert(sd.serdeInfo().serializationLibrary() == "serde.Lib")
    assert(sd.serdeInfo().parameters().asScala == Map("sk" -> "sv"))
    assert(sd.columns().asScala.map(c => (c.name(), c.`type`(), c.comment())) ==
      Seq(("c1", "string", "first"), ("c2", "int", null)))
    assert(in.partitionKeys().asScala.map(c => (c.name(), c.`type`())) == Seq(("p", "string")))
    assert(api.getTable("db", "t") == Some(entry))
  }

  test("getTable on a sparse table is null-safe") {
    val stub = new Stub {
      override def getTable(r: GetTableRequest): GetTableResponse = GetTableResponse.builder().table(sdkTable("t")).build()
    }
    val t = new SdkGlueApi(stub, None, GlueRequestObserver.Noop).getTable("db", "t").get
    assert(t.versionId.isEmpty && t.parameters.isEmpty && t.partitionKeys.isEmpty && t.storage.columns.isEmpty)
    assert(t.storage.location.isEmpty && t.storage.serdeLibrary.isEmpty)
  }

  test("database round trip") {
    val created = ArrayBuffer.empty[CreateDatabaseRequest]
    val stub = new Stub {
      override def createDatabase(r: CreateDatabaseRequest): CreateDatabaseResponse = { created += r; CreateDatabaseResponse.builder().build() }
      override def getDatabase(r: GetDatabaseRequest): GetDatabaseResponse = {
        val in = created.head.databaseInput()
        GetDatabaseResponse.builder().database(Database.builder().name(in.name()).description(in.description())
          .locationUri(in.locationUri()).parameters(in.parameters()).build()).build()
      }
    }
    val db = GlueDatabaseEntry("db", Some("d"), Some("s3://bucket-a/db"), Map("a" -> "b"))
    val api = new SdkGlueApi(stub, Some(Cat), GlueRequestObserver.Noop)
    api.createDatabase(db)
    assert(api.getDatabase("db") == Some(db))
  }
}
