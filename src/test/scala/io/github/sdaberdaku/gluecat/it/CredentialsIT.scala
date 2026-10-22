package io.github.sdaberdaku.gluecat.it

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.Base64

import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.iam.IamClient
import software.amazon.awssdk.services.iam.model.CreateRoleRequest

import io.github.sdaberdaku.gluecat.glue.StaticKeyCredentialsProvider

/** The three credential routes, each landing in its own MiniStack account (MiniStack keys state by access key id and
  * maps STS sessions to the role's account). `acct_a` uses the default chain with the system properties (111). */
@RunWith(classOf[JUnitRunner])
class CredentialsIT extends SparkIT(Named) {
  override protected def otherCatalogs: Map[String, Map[String, String]] = Map(
    "acct_b" -> Map(
      "glue.id" -> "222222222222",
      "client.assume-role.arn" -> "arn:aws:iam::222222222222:role/glue-access",
      "client.assume-role.region" -> "eu-central-1"),
    "acct_c" -> Map(
      "glue.id" -> "333333333333",
      "client.credentials-provider" -> classOf[StaticKeyCredentialsProvider].getName,
      "client.credentials-provider.access-key" -> "333333333333",
      "client.credentials-provider.secret-key" -> "test"),
    "acct_irsa" -> Map("glue.id" -> "222222222222"))

  private val glueB = new GlueInspector("222222222222")
  private val glueC = new GlueInspector("333333333333")

  override def afterAll(): Unit =
    try { glueB.close(); glueC.close() }
    finally super.afterAll()

  private def databases(catalog: String): Seq[String] =
    spark.sql(s"SHOW DATABASES IN $catalog").collect().map(_.getString(0)).toList

  test("default chain") {
    spark.sql("CREATE DATABASE acct_a.db_a")
    assert(databases("acct_a").contains("db_a"))
    assert(glue.listDatabaseNames().contains("db_a"))
  }

  test("assume role") {
    // MiniStack (AUTH off) needs no IAM role for AssumeRole.
    spark.sql("CREATE DATABASE acct_b.db_b")
    assert(glueB.listDatabaseNames().contains("db_b"))
    assert(!glue.listDatabaseNames().contains("db_b"))
  }

  test("custom provider class") {
    spark.sql("CREATE DATABASE acct_c.db_c")
    assert(glueC.listDatabaseNames().contains("db_c"))
    assert(!glue.listDatabaseNames().contains("db_c"))
  }

  test("IRSA-style web identity") {
    // Environment keys come before web identity in the default chain.
    assume(sys.env.get("AWS_ACCESS_KEY_ID").isEmpty)
    val iam = IamClient.builder().region(Region.AWS_GLOBAL).endpointOverride(URI.create(MiniStack.Endpoint))
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("222222222222", "test"))).build()
    try iam.createRole(CreateRoleRequest.builder().roleName("glue-irsa").assumeRolePolicyDocument(
      """{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Federated":"arn:aws:iam::222222222222:oidc-provider/oidc.example.com/id/TEST"},"Action":"sts:AssumeRoleWithWebIdentity"}]}""").build())
    finally iam.close()
    glueB.createDatabase("db_irsa")

    val b64 = Base64.getUrlEncoder.withoutPadding()
    def part(json: String) = b64.encodeToString(json.getBytes(UTF_8))
    val exp = System.currentTimeMillis() / 1000 + 3600
    val token = Files.createTempFile("gluecat-irsa", ".jwt")
    Files.write(token, (part("""{"alg":"RS256","typ":"JWT"}""") + "." + part(
      s"""{"iss":"https://oidc.example.com/id/TEST","sub":"system:serviceaccount:ns:sa","aud":"sts.amazonaws.com","exp":$exp}""") +
      ".sig").getBytes(UTF_8))

    val keys = Seq("aws.accessKeyId", "aws.secretAccessKey", "aws.webIdentityTokenFile", "aws.roleArn", "aws.roleSessionName")
    val saved = keys.map(k => k -> Option(System.getProperty(k)))
    try {
      System.clearProperty("aws.accessKeyId")
      System.clearProperty("aws.secretAccessKey")
      System.setProperty("aws.webIdentityTokenFile", token.toString)
      System.setProperty("aws.roleArn", "arn:aws:iam::222222222222:role/glue-irsa")
      System.setProperty("aws.roleSessionName", "irsa")
      startSession() // new catalogs, so acct_irsa resolves the default chain now
      assert(databases("acct_irsa").contains("db_irsa"))
    } finally {
      saved.foreach {
        case (k, Some(v)) => System.setProperty(k, v)
        case (k, None) => System.clearProperty(k)
      }
      Files.deleteIfExists(token)
      startSession()
    }
  }
}
