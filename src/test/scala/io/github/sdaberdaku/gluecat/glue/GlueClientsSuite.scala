package io.github.sdaberdaku.gluecat.glue

import scala.collection.JavaConverters._

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider

import io.github.sdaberdaku.gluecat.GlueCatalogOptions

@RunWith(classOf[JUnitRunner])
class GlueClientsSuite extends AnyFunSuite {
  private def opts(kv: (String, String)*): GlueCatalogOptions =
    GlueCatalogOptions.parse("acct_a", kv.toMap.asJava)

  test("credentialsProvider: assume role wins over default chain") {
    val p = GlueClients.credentialsProvider(opts(
      "client.assume-role.arn" -> "arn:aws:iam::222222222222:role/glue-access", "client.region" -> "eu-central-1"))
    try assert(p.isInstanceOf[StsAssumeRoleCredentialsProvider])
    finally p match { case c: AutoCloseable => c.close(); case _ => () }
  }

  test("credentialsProvider: custom class wins over assume role and uses create(Map)") {
    val p = GlueClients.credentialsProvider(opts(
      "client.credentials-provider" -> classOf[StaticKeyCredentialsProvider].getName,
      "client.credentials-provider.access-key" -> "AKIA1",
      "client.credentials-provider.secret-key" -> "secret1",
      "client.assume-role.arn" -> "arn:aws:iam::222222222222:role/glue-access"))
    assert(p.resolveCredentials().accessKeyId() == "AKIA1")
  }

  test("credentialsProvider: custom class via no-arg create()") {
    val p = GlueClients.credentialsProvider(opts(
      "client.credentials-provider" -> classOf[NoArgCreateCredentialsProvider].getName))
    assert(p.resolveCredentials().accessKeyId() == "AKIA2")
  }

  test("credentialsProvider: custom class via constructor") {
    val p = GlueClients.credentialsProvider(opts(
      "client.credentials-provider" -> classOf[ConstructorOnlyCredentialsProvider].getName))
    assert(p.resolveCredentials().accessKeyId() == "AKIA3")
  }

  test("credentialsProvider: a class that is not a provider is rejected naming class and catalog") {
    val e = intercept[IllegalArgumentException](GlueClients.credentialsProvider(opts(
      "client.credentials-provider" -> classOf[NotAProvider].getName)))
    assert(e.getMessage.contains(classOf[NotAProvider].getName))
  }

  test("build applies endpoint and region") {
    val client = GlueClients.build(opts("glue.endpoint" -> "http://localhost:4566", "client.region" -> "eu-central-1",
      "client.credentials-provider" -> classOf[StaticKeyCredentialsProvider].getName,
      "client.credentials-provider.access-key" -> "AKIA1", "client.credentials-provider.secret-key" -> "s"))
    try {
      val conf = client.serviceClientConfiguration()
      assert(conf.endpointOverride().get.toString == "http://localhost:4566")
      assert(conf.region.id == "eu-central-1")
    } finally client.close()
  }
}
