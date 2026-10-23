package io.github.sdaberdaku.gluecat.glue

import scala.collection.JavaConverters._

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import io.github.sdaberdaku.gluecat.GlueCatalogOptions

@RunWith(classOf[JUnitRunner])
class HttpClientsSuite extends AnyFunSuite {
  private def parse(kv: (String, String)*): GlueCatalogOptions =
    GlueCatalogOptions.parse("acct_a", kv.toMap.asJava)

  test("no http-client keys: SDK default") { assert(GlueClients.httpClient(parse()).isEmpty) }

  test("apache from implied type with pool size") {
    val c = GlueClients.httpClient(parse(
      "http-client.apache.max-connections" -> "500", "http-client.apache.socket-timeout-ms" -> "120000")).get
    assert(c.getClass.getName.startsWith("software.amazon.awssdk.http.apache.ApacheHttpClient"))
    c.close()
  }

  test("apache with every setting and a proxy") {
    val c = GlueClients.httpClient(parse(
      "http-client.proxy-endpoint" -> "http://proxy.example.com:3128",
      "http-client.proxy-use-system-property-values" -> "false",
      "http-client.proxy-use-environment-variable-values" -> "false",
      "http-client.apache.socket-timeout-ms" -> "1000", "http-client.apache.connection-timeout-ms" -> "1000",
      "http-client.apache.connection-acquisition-timeout-ms" -> "1000",
      "http-client.apache.connection-max-idle-time-ms" -> "1000",
      "http-client.apache.connection-time-to-live-ms" -> "1000",
      "http-client.apache.expect-continue-enabled" -> "false", "http-client.apache.max-connections" -> "10",
      "http-client.apache.tcp-keep-alive-enabled" -> "true",
      "http-client.apache.use-idle-connection-reaper-enabled" -> "false")).get
    c.close()
  }

  test("urlconnection explicit") {
    val c = GlueClients.httpClient(parse(
      "http-client.type" -> "urlconnection", "http-client.urlconnection.socket-timeout-ms" -> "1000")).get
    assert(c.getClass.getName.startsWith("software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient"))
    c.close()
  }

  test("a type without its keys still builds, ignoring the other type's keys") {
    val c = GlueClients.httpClient(parse("http-client.type" -> "urlconnection",
      "http-client.apache.max-connections" -> "5")).get
    assert(c.getClass.getName.startsWith("software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient"))
    c.close()
  }

  test("a Glue client builds with each type") {
    Seq("apache", "urlconnection").foreach { t =>
      GlueClients.build(parse("http-client.type" -> t, "client.region" -> "eu-central-1")).close()
    }
  }
}
