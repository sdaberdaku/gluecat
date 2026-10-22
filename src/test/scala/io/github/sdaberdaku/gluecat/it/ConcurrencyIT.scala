package io.github.sdaberdaku.gluecat.it

import scala.collection.JavaConverters._

import org.apache.spark.sql.connector.catalog.{Identifier, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import io.github.sdaberdaku.gluecat.GlueCatalogOptions
import io.github.sdaberdaku.gluecat.catalog.{GlueCatalogConcurrentModificationException, GlueCoreCatalog}
import io.github.sdaberdaku.gluecat.glue.SdkGlueApi

/** Two writers on the same Glue entry: the one holding a stale VersionId loses. No Spark session. */
@RunWith(classOf[JUnitRunner])
class ConcurrencyIT extends AnyFunSuite with BeforeAndAfterAll {
  private val options = GlueCatalogOptions.parse("acct_a", Map(
    "glue.id" -> "111111111111", "client.region" -> "eu-central-1", "glue.endpoint" -> MiniStack.Endpoint).asJava)
  private def core() = new GlueCoreCatalog("acct_a", SdkGlueApi(options), options, () => 4000)
  private val glue = new GlueInspector("111111111111")
  private val t = Identifier.of(Array("db"), "t")

  override def beforeAll(): Unit = {
    MiniStack.awaitReady()
    MiniStack.reset()
  }

  override def afterAll(): Unit = glue.close()

  test("an alter built from a stale read fails with the expected VersionId and changes nothing") {
    val first = core()
    val second = core()
    first.createNamespace(Array("db"), Map.empty[String, String].asJava)
    first.createTable(t, new StructType().add("id", IntegerType), Array.empty[Transform],
      Map("provider" -> "parquet", "location" -> "s3://bucket-a/db/t").asJava)

    val staleEntry = second.loadEntry(t).get
    first.alterTable(t, TableChange.setProperty("a", "1"))

    val e = intercept[GlueCatalogConcurrentModificationException](second.alterEntry(t, staleEntry,
      Seq(TableChange.setProperty("b", "2"), TableChange.addColumn(Array("x"), StringType))))
    assert(e.getMessage.contains(s"expected VersionId ${staleEntry.versionId.get}"))

    val entry = glue.getTable("db", "t")
    assert(entry.parameters().get("a") == "1")
    assert(!entry.parameters().containsKey("b"))
    assert(entry.storageDescriptor().columns().asScala.map(_.name()) == Seq("id"))
  }
}
