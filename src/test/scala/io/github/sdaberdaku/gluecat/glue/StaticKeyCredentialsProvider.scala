package io.github.sdaberdaku.gluecat.glue

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, AwsCredentials, AwsCredentialsProvider}

/** Test provider reachable through all three instantiation routes. */
class StaticKeyCredentialsProvider(accessKey: String, secretKey: String) extends AwsCredentialsProvider {
  override def resolveCredentials(): AwsCredentials = AwsBasicCredentials.create(accessKey, secretKey)
}

object StaticKeyCredentialsProvider {
  def create(options: java.util.Map[String, String]): StaticKeyCredentialsProvider =
    new StaticKeyCredentialsProvider(options.get("access-key"), options.get("secret-key"))
}

/** Has static create() but no create(Map). */
class NoArgCreateCredentialsProvider private () extends AwsCredentialsProvider {
  override def resolveCredentials(): AwsCredentials = AwsBasicCredentials.create("AKIA2", "secret2")
}

object NoArgCreateCredentialsProvider {
  def create(): NoArgCreateCredentialsProvider = new NoArgCreateCredentialsProvider
}

/** Not a credentials provider. */
class NotAProvider

/** No create method at all: instantiated through its no-arg constructor. */
class ConstructorOnlyCredentialsProvider extends AwsCredentialsProvider {
  override def resolveCredentials(): AwsCredentials = AwsBasicCredentials.create("AKIA3", "secret3")
}
