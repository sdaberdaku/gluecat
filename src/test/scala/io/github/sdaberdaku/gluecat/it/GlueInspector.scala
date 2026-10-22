package io.github.sdaberdaku.gluecat.it

import java.net.URI

import scala.collection.JavaConverters._

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.glue.GlueClient
import software.amazon.awssdk.services.glue.model._

/** Test-owned view of one MiniStack account's Glue store, independent of the library under test.
  * MiniStack keys state per account by the access key id. */
final class GlueInspector(accountKey: String) extends AutoCloseable {
  private val client = GlueClient.builder()
    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accountKey, "test")))
    .endpointOverride(URI.create(MiniStack.Endpoint))
    .region(Region.EU_CENTRAL_1)
    .build()

  def getTable(db: String, t: String): Table =
    client.getTable(GetTableRequest.builder().databaseName(db).name(t).build()).table()

  def listTableNames(db: String): Seq[String] =
    client.getTables(GetTablesRequest.builder().databaseName(db).build()).tableList().asScala.map(_.name()).toList

  def listDatabaseNames(): Seq[String] =
    client.getDatabases(GetDatabasesRequest.builder().build()).databaseList().asScala.map(_.name()).toList

  def createRawTable(db: String, input: TableInput): Unit =
    client.createTable(CreateTableRequest.builder().databaseName(db).tableInput(input).build())

  def createDatabase(db: String): Unit =
    client.createDatabase(CreateDatabaseRequest.builder().databaseInput(DatabaseInput.builder().name(db).build()).build())

  override def close(): Unit = client.close()
}
