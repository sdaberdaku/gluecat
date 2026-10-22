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
}
