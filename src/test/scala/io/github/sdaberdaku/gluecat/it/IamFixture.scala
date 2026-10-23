package io.github.sdaberdaku.gluecat.it

import java.net.URI

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.iam.IamClient
import software.amazon.awssdk.services.iam.model._

/** IAM roles and users in MiniStack (`AUTH=true`), created with the account's root key. Idempotent: an existing role
  * or user is kept and its inline policy `glue` is put again. */
object IamFixture {
  /** The Glue actions gluecat calls, exactly. */
  val GlueActions: Seq[String] = Seq(
    "glue:CreateDatabase", "glue:GetDatabase", "glue:GetDatabases", "glue:UpdateDatabase", "glue:DeleteDatabase",
    "glue:CreateTable", "glue:GetTable", "glue:GetTables", "glue:UpdateTable", "glue:DeleteTable")

  /** An identity policy allowing `actions` on every resource (MiniStack's Glue resource ARNs are coarse). */
  private def gluePolicy(actions: Seq[String]): String =
    s"""{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":[${quoted(actions)}],"Resource":"*"}]}"""

  /** A role in `account` that the roots of `trustedAccounts` (empty: `account` itself) may assume, with `actions` as
    * its inline policy. */
  def ensureRole(account: String, roleName: String = "gluecat-test", actions: Seq[String] = GlueActions,
      trustedAccounts: Seq[String] = Nil): String = withIam(account) { iam =>
    val trusted = if (trustedAccounts.isEmpty) Seq(account) else trustedAccounts
    val trust = s"""{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":[${
      quoted(trusted.map(a => s"arn:aws:iam::$a:root"))}]},"Action":"sts:AssumeRole"}]}"""
    ignoreExisting(iam.createRole(CreateRoleRequest.builder().roleName(roleName).assumeRolePolicyDocument(trust).build()))
    iam.putRolePolicy(PutRolePolicyRequest.builder().roleName(roleName).policyName("glue")
      .policyDocument(gluePolicy(actions)).build())
    s"arn:aws:iam::$account:role/$roleName"
  }

  /** A user in `account` with `actions` as its inline policy, and a new access key (id, secret) for it. */
  def ensureUser(account: String, userName: String, actions: Seq[String]): (String, String) = withIam(account) { iam =>
    ignoreExisting(iam.createUser(CreateUserRequest.builder().userName(userName).build()))
    iam.putUserPolicy(PutUserPolicyRequest.builder().userName(userName).policyName("glue")
      .policyDocument(gluePolicy(actions)).build())
    val key = iam.createAccessKey(CreateAccessKeyRequest.builder().userName(userName).build()).accessKey()
    (key.accessKeyId(), key.secretAccessKey())
  }

  private def quoted(values: Seq[String]): String = values.map("\"" + _ + "\"").mkString(",")

  private def ignoreExisting(create: => Any): Unit =
    try create catch { case _: EntityAlreadyExistsException => () }

  private def withIam[T](account: String)(f: IamClient => T): T = {
    val iam = IamClient.builder().region(Region.AWS_GLOBAL).endpointOverride(URI.create(MiniStack.Endpoint))
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(account, "test"))).build()
    try f(iam) finally iam.close()
  }
}
