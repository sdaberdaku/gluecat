package io.github.sdaberdaku.gluecat

import scala.collection.JavaConverters._

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GlueCatalogOptionsSuite extends AnyFunSuite {
  private def parse(kv: (String, String)*): GlueCatalogOptions =
    GlueCatalogOptions.parse("acct_a", kv.toMap.asJava)

  test("defaults") {
    val o = parse()
    assert(o.glueId.isEmpty)
    assert(o.skipArchive)
    assert(o.defaultNamespace == "default")
    assert(o.assumeRoleSessionName == "gluecat")
    assert(o.assumeRoleTimeoutSec == 3600)
    assert(o.credentialsProviderOptions.isEmpty)
  }

  test("reads every key") {
    val o = parse(
      "glue.id" -> "111111111111",
      "glue.endpoint" -> "http://localhost:4566",
      "glue.skip-archive" -> "false",
      "warehouse" -> "s3://bucket-a/wh",
      "default-namespace" -> "main",
      "client.region" -> "eu-central-1",
      "client.credentials-provider" -> "x.Y",
      "client.credentials-provider.access-key" -> "k",
      "client.assume-role.arn" -> "arn:aws:iam::222222222222:role/glue-access",
      "client.assume-role.external-id" -> "ext",
      "client.assume-role.session-name" -> "s",
      "client.assume-role.region" -> "us-east-1",
      "client.assume-role.timeout-sec" -> "900")
    assert(o.glueId.contains("111111111111"))
    assert(o.endpoint.contains("http://localhost:4566"))
    assert(!o.skipArchive)
    assert(o.warehouse.contains("s3://bucket-a/wh"))
    assert(o.defaultNamespace == "main")
    assert(o.region.contains("eu-central-1"))
    assert(o.credentialsProvider.contains("x.Y"))
    assert(o.credentialsProviderOptions == Map("access-key" -> "k"))
    assert(o.assumeRoleArn.contains("arn:aws:iam::222222222222:role/glue-access"))
    assert(o.assumeRoleExternalId.contains("ext"))
    assert(o.assumeRoleSessionName == "s")
    assert(o.assumeRoleRegion.contains("us-east-1"))
    assert(o.assumeRoleTimeoutSec == 900)
  }

  test("blank values count as absent") {
    assert(parse("glue.id" -> "  ").glueId.isEmpty)
  }

  test("default-namespace must be single level") {
    val e = intercept[IllegalArgumentException](parse("default-namespace" -> "a.b"))
    assert(e.getMessage.contains("default-namespace"))
  }

  test("timeout must be a positive integer") {
    intercept[IllegalArgumentException](parse("client.assume-role.timeout-sec" -> "abc"))
  }

  test("unknown glue.* and client.* keys do not fail") {
    parse("glue.bogus" -> "1", "client.bogus" -> "2", "some.delta.option" -> "3")
  }

  test("hadoop.* keys are collected without the prefix") {
    val o = parse("hadoop.fs.s3a.endpoint" -> "http://localhost:4566", "hadoop.fs.s3a.path.style.access" -> "true", "hadoop.blank" -> " ")
    assert(o.hadoop == Map("fs.s3a.endpoint" -> "http://localhost:4566", "fs.s3a.path.style.access" -> "true"))
  }

  test("hadoop.* keys are not reported as unknown") {
    assert(parse("hadoop.anything.goes" -> "1").hadoop == Map("anything.goes" -> "1"))
  }

  test("http-client keys") {
    val o = parse("http-client.type" -> "apache", "http-client.apache.max-connections" -> "500",
      "http-client.apache.socket-timeout-ms" -> "120000", "http-client.proxy-endpoint" -> "http://proxy:3128")
    assert(o.httpClient.clientType.contains("apache"))
    assert(o.httpClient.apacheMaxConnections.contains(500))
    assert(o.httpClient.apacheSocketTimeoutMs.contains(120000L))
    assert(o.httpClient.proxyEndpoint.contains("http://proxy:3128"))
  }

  test("http-client.type is implied by the keys present") {
    assert(parse("http-client.apache.max-connections" -> "500").httpClient.effectiveType.contains("apache"))
    assert(parse("http-client.proxy-endpoint" -> "http://p:1").httpClient.effectiveType.contains("apache"))
    assert(parse("http-client.urlconnection.socket-timeout-ms" -> "1000").httpClient.effectiveType.contains("urlconnection"))
    assert(parse().httpClient.effectiveType.isEmpty)
  }

  test("proxy keys go with either client type") {
    val o = parse("http-client.proxy-endpoint" -> "http://p:1", "http-client.urlconnection.socket-timeout-ms" -> "1000")
    assert(o.httpClient.effectiveType.contains("urlconnection"))
    assert(parse("http-client.proxy-endpoint" -> "http://p:1", "http-client.apache.max-connections" -> "5")
      .httpClient.effectiveType.contains("apache"))
  }

  test("http-client errors") {
    assert(intercept[IllegalArgumentException](parse("http-client.type" -> "netty")).getMessage.contains("'apache' or 'urlconnection'"))
    assert(intercept[IllegalArgumentException](parse("http-client.apache.max-connections" -> "1",
      "http-client.urlconnection.socket-timeout-ms" -> "1")).getMessage.contains("set http-client.type"))
    assert(intercept[IllegalArgumentException](parse("http-client.apache.max-connections" -> "x")).getMessage
      .contains("http-client.apache.max-connections must be an integer, got 'x'"))
  }

  test("unknown http-client key warns and is ignored") {
    assert(parse("http-client.apache.bogus" -> "1").httpClient.effectiveType.isEmpty)
  }
}
