package io.github.sdaberdaku.gluecat

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
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
  private val noSession = SessionHadoop(_ => None, _ => None)
  /** A session where only `m` is set (user-set and effective views coincide). */
  private def session(m: Map[String, String]) = SessionHadoop(m.get, m.get)

  test("derives the S3A assumed-role keys from the catalog role") {
    assert(S3RoleDefaults.derive(role, noSession, "3.4.1") == Map(
      "fs.s3a.aws.credentials.provider" -> Assumed,
      "fs.s3a.assumed.role.arn" -> Arn,
      "fs.s3a.assumed.role.session.name" -> "sess",
      "fs.s3a.assumed.role.session.duration" -> "900s",
      "fs.s3a.assumed.role.external.id" -> "ext-1"))
  }

  test("no external id: that row is absent") {
    val d = S3RoleDefaults.derive(parse("client.assume-role.arn" -> Arn), noSession)
    assert(d.keySet == Set("fs.s3a.aws.credentials.provider", "fs.s3a.assumed.role.arn",
      "fs.s3a.assumed.role.session.name", "fs.s3a.assumed.role.session.duration"))
  }

  private val BaseKey = "fs.s3a.assumed.role.credentials.provider"
  private val MainKey = "fs.s3a.aws.credentials.provider"
  private val Web = "software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider"

  test("a user-set main provider is copied to the base key") {
    assert(S3RoleDefaults.derive(role, session(Map(MainKey -> Web)), "3.4.1")(BaseKey) == Web)
  }

  test("the assumed-role provider is removed from the copied list, others keep their order; alone, the row is absent") {
    val list = s"A,$Assumed, B ,C"
    assert(S3RoleDefaults.derive(role, session(Map(MainKey -> list)), "3.4.1")(BaseKey) == "A,B,C")
    assert(!S3RoleDefaults.derive(role, session(Map(MainKey -> Assumed)), "3.4.1").contains(BaseKey))
    assert(S3RoleDefaults.derive(role, session(Map(MainKey -> s"x.y.$Assumed,A")), "3.4.1")(BaseKey) == "A")
  }

  test("a user-set base key is not overridden") {
    val set = Map(MainKey -> "X", BaseKey -> "Y")
    assert(!S3RoleDefaults.derive(role, session(set), "3.4.1").contains(BaseKey))
  }

  test("never derives the endpoint region, even with client.region") {
    assert(!S3RoleDefaults.derive(role, noSession, "3.4.1").contains("fs.s3a.endpoint.region"))
  }

  test("external id on Hadoop 3.3 derives nothing and is flagged; 3.4 and no external id derive in full") {
    assert(S3RoleDefaults.derive(role, noSession, "3.3.4").isEmpty)
    assert(S3RoleDefaults.externalIdUnsupported(role, "3.3.4"))
    val full = S3RoleDefaults.derive(role, noSession, "3.4.1")
    assert(full("fs.s3a.assumed.role.external.id") == "ext-1")
    assert(full.contains("fs.s3a.assumed.role.arn"))
    assert(!S3RoleDefaults.externalIdUnsupported(role, "3.4.1"))
    val noExt = parse("client.assume-role.arn" -> Arn)
    assert(S3RoleDefaults.derive(noExt, noSession, "3.3.4").contains("fs.s3a.assumed.role.arn"))
    assert(!S3RoleDefaults.externalIdUnsupported(noExt, "3.3.4"))
    assert(!S3RoleDefaults.externalIdUnsupported(parse(), "3.3.4"))
  }

  test("never derives the STS endpoint keys") {
    assert(!S3RoleDefaults.derive(role, noSession, "3.4.1").keys.exists(_.startsWith("fs.s3a.assumed.role.sts.")))
  }

  test("nothing without a role, nothing with a custom provider class") {
    assert(S3RoleDefaults.derive(parse(), noSession).isEmpty)
    assert(S3RoleDefaults.derive(parse("client.assume-role.arn" -> "arn:aws:iam::1:role/r",
      "client.credentials-provider" -> "com.example.P"), noSession).isEmpty)
  }

  test("explicit hadoop.* wins over derived") {
    val o = parse("client.assume-role.arn" -> Arn, "hadoop.fs.s3a.aws.credentials.provider" -> "com.example.Base")
    val e = S3RoleDefaults.effective(o, noSession, "3.4.1")
    assert(e("fs.s3a.aws.credentials.provider") == "com.example.Base")
    assert(e("fs.s3a.assumed.role.arn") == Arn)
  }

  test("userSet ignores Hadoop's built-in defaults") {
    val conf = new Configuration()
    assert(conf.get(BaseKey) != null)
    assert(SessionHadoop(conf).userSet(BaseKey).isEmpty)
    assert(SessionHadoop(conf).userSet(MainKey).isEmpty)
  }

  test("userSet sees values the user set") {
    val conf = new Configuration()
    conf.set(MainKey, "X")
    assert(SessionHadoop(conf).userSet(MainKey).contains("X"))
  }

  test("with nothing set by the user the base is Hadoop's default main provider list") {
    val conf = new Configuration()
    val default = conf.get(MainKey)
    assert(default != null && default.nonEmpty)
    val d = S3RoleDefaults.derive(role, SessionHadoop(conf), "3.4.1")
    assert(d(BaseKey) == default.split(",").map(_.trim).filter(_.nonEmpty).mkString(","))
    assert(d(MainKey) == Assumed)
  }

  test("a user-set main provider on a real Configuration is copied") {
    val conf = new Configuration()
    conf.set(MainKey, Web)
    assert(S3RoleDefaults.derive(role, SessionHadoop(conf), "3.4.1")(BaseKey) == Web)
  }

  test("an unparseable Hadoop version derives normally and warns once; no crash without an external id") {
    for (v <- Seq("Unknown", "", "3")) {
      assert(!S3RoleDefaults.externalIdUnsupported(role, v))
      assert(S3RoleDefaults.derive(role, noSession, v).contains("fs.s3a.assumed.role.external.id"))
      val w = S3RoleDefaults.warnings("acct_a", role, v)
      assert(w.size == 1 && w.head.contains("could not be parsed"))
      assert(S3RoleDefaults.warnings("acct_a", parse(), v).isEmpty)
      assert(S3RoleDefaults.derive(parse("client.assume-role.arn" -> Arn), noSession, v).nonEmpty)
    }
  }

  test("warning text: one on Hadoop 3.3 naming the remedy, none on 3.4") {
    val w = S3RoleDefaults.warnings("acct_a", role, "3.3.4")
    assert(w.size == 1 && w.head.startsWith("gluecat catalog 'acct_a': Hadoop 3.3.4 cannot send") &&
      w.head.contains("or use Spark 4."))
    assert(S3RoleDefaults.warnings("acct_a", role, "3.4.1").isEmpty)
  }
}
