package io.github.sdaberdaku.gluecat.glue

import java.net.URI

import scala.collection.JavaConverters._

import software.amazon.awssdk.auth.credentials.{AwsCredentialsProvider, DefaultCredentialsProvider}
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryMode
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.glue.GlueClient
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest

import io.github.sdaberdaku.gluecat.GlueCatalogOptions

object GlueClients {
  def build(options: GlueCatalogOptions): GlueClient = {
    val b = GlueClient.builder()
      .credentialsProvider(credentialsProvider(options))
      .overrideConfiguration(ClientOverrideConfiguration.builder().retryPolicy(RetryMode.STANDARD).build())
    options.region.foreach(r => b.region(Region.of(r)))
    options.endpoint.foreach(e => b.endpointOverride(URI.create(e)))
    b.build()
  }

  /** Precedence: custom class, then assume role, then the default chain. */
  def credentialsProvider(options: GlueCatalogOptions): AwsCredentialsProvider =
    options.credentialsProvider.map(custom(_, options))
      .orElse(options.assumeRoleArn.map(assumeRole(_, options)))
      .getOrElse(defaultChain())

  // A chain per catalog: DefaultCredentialsProvider.create() is one JVM-wide instance that keeps the first provider
  // that worked and reads the web identity settings only once.
  private def defaultChain(): AwsCredentialsProvider = DefaultCredentialsProvider.builder().build()

  private def custom(className: String, options: GlueCatalogOptions): AwsCredentialsProvider = {
    def fail(cause: Throwable, detail: String) = new IllegalArgumentException(
      s"client.credentials-provider '$className' $detail", cause)
    val instance: AnyRef =
      try {
        // Same class loader Spark uses for catalog plugins.
        val loader = Option(Thread.currentThread.getContextClassLoader).getOrElse(getClass.getClassLoader)
        val cls = Class.forName(className, true, loader)
        try cls.getMethod("create", classOf[java.util.Map[_, _]]).invoke(null, options.credentialsProviderOptions.asJava)
        catch {
          case _: NoSuchMethodException =>
            try cls.getMethod("create").invoke(null)
            catch { case _: NoSuchMethodException => cls.getConstructor().newInstance().asInstanceOf[AnyRef] }
        }
      } catch {
        case e: java.lang.reflect.InvocationTargetException =>
          throw fail(if (e.getCause != null) e.getCause else e, s"failed to instantiate: ${Option(e.getCause).getOrElse(e)}")
        case e: ReflectiveOperationException => throw fail(e, s"cannot be loaded or instantiated: $e")
      }
    instance match {
      case p: AwsCredentialsProvider => p
      case _ => throw new IllegalArgumentException(
        s"client.credentials-provider '$className' does not implement software.amazon.awssdk.auth.credentials.AwsCredentialsProvider")
    }
  }

  private def assumeRole(arn: String, options: GlueCatalogOptions): AwsCredentialsProvider = {
    val sts = StsClient.builder().credentialsProvider(defaultChain())
    options.assumeRoleRegion.orElse(options.region).foreach(r => sts.region(Region.of(r)))
    val req = AssumeRoleRequest.builder().roleArn(arn).roleSessionName(options.assumeRoleSessionName)
      .durationSeconds(options.assumeRoleTimeoutSec)
    options.assumeRoleExternalId.foreach(req.externalId)
    StsAssumeRoleCredentialsProvider.builder().stsClient(sts.build()).refreshRequest(req.build())
      .asyncCredentialUpdateEnabled(true).build()
  }
}
