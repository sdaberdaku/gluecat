package io.github.sdaberdaku.gluecat

import scala.collection.JavaConverters._

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class S3RoleDefaultsSuite extends AnyFunSuite {
  private def parse(kv: (String, String)*): GlueCatalogOptions =
    GlueCatalogOptions.parse("acct_a", kv.toMap.asJava)

  private val Arn = "arn:aws:iam::111111111111:role/glue-access"
  private val Assumed = "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider"
  private val role = parse("client.assume-role.arn" -> Arn, "client.region" -> "eu-central-1",
    "client.assume-role.external-id" -> "ext-1", "client.assume-role.session-name" -> "sess",
    "client.assume-role.timeout-sec" -> "900")
  private def noSession(k: String): Option[String] = None

  test("derives the S3A assumed-role keys from the catalog role") {
    assert(S3RoleDefaults.derive(role, noSession) == Map(
      "fs.s3a.aws.credentials.provider" -> Assumed,
      "fs.s3a.assumed.role.arn" -> Arn,
      "fs.s3a.assumed.role.session.name" -> "sess",
      "fs.s3a.assumed.role.session.duration" -> "900s",
      "fs.s3a.assumed.role.external.id" -> "ext-1",
      "fs.s3a.endpoint.region" -> "eu-central-1"))
  }

  test("no external id, no region: those rows are absent") {
    val d = S3RoleDefaults.derive(parse("client.assume-role.arn" -> Arn), noSession)
    assert(d.keySet == Set("fs.s3a.aws.credentials.provider", "fs.s3a.assumed.role.arn",
      "fs.s3a.assumed.role.session.name", "fs.s3a.assumed.role.session.duration"))
  }

  test("the base provider is copied from the session when the session sets only the main provider") {
    val session = Map("fs.s3a.aws.credentials.provider" ->
      "software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider")
    assert(S3RoleDefaults.derive(role, session.get)("fs.s3a.assumed.role.credentials.provider") ==
      session("fs.s3a.aws.credentials.provider"))
  }

  test("the base provider is not copied when the session already sets it, or when the session provider is the assumed-role provider") {
    val set = Map("fs.s3a.aws.credentials.provider" -> "X", "fs.s3a.assumed.role.credentials.provider" -> "Y")
    assert(!S3RoleDefaults.derive(role, set.get).contains("fs.s3a.assumed.role.credentials.provider"))
    val recursive = Map("fs.s3a.aws.credentials.provider" -> Assumed)
    assert(!S3RoleDefaults.derive(role, recursive.get).contains("fs.s3a.assumed.role.credentials.provider"))
  }

  test("never derives the STS endpoint keys") {
    assert(!S3RoleDefaults.derive(role, noSession).keys.exists(_.startsWith("fs.s3a.assumed.role.sts.")))
  }

  test("nothing without a role, nothing with a custom provider class") {
    assert(S3RoleDefaults.derive(parse(), noSession).isEmpty)
    assert(S3RoleDefaults.derive(parse("client.assume-role.arn" -> "arn:aws:iam::1:role/r",
      "client.credentials-provider" -> "com.example.P"), noSession).isEmpty)
  }

  test("explicit hadoop.* wins over derived") {
    val o = parse("client.assume-role.arn" -> Arn, "hadoop.fs.s3a.aws.credentials.provider" -> "com.example.Base")
    val e = S3RoleDefaults.effective(o, noSession)
    assert(e("fs.s3a.aws.credentials.provider") == "com.example.Base")
    assert(e("fs.s3a.assumed.role.arn") == Arn)
  }
}
