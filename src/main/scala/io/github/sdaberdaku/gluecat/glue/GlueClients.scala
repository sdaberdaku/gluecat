package io.github.sdaberdaku.gluecat.glue

import java.net.URI
import java.time.Duration

import scala.collection.JavaConverters._

import software.amazon.awssdk.auth.credentials.{AwsCredentialsProvider, DefaultCredentialsProvider}
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryMode
import software.amazon.awssdk.http.SdkHttpClient
import software.amazon.awssdk.http.apache.{ApacheHttpClient, ProxyConfiguration => ApacheProxy}
import software.amazon.awssdk.http.urlconnection.{ProxyConfiguration => UrlConnectionProxy, UrlConnectionHttpClient}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.glue.GlueClient
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest

import org.slf4j.LoggerFactory

import io.github.sdaberdaku.gluecat.GlueCatalogOptions

object GlueClients {
  private val log = LoggerFactory.getLogger(getClass)

  def build(options: GlueCatalogOptions): GlueClient = {
    val shared = httpClient(options) // one instance for the Glue and the STS client
    val b = GlueClient.builder()
      .credentialsProvider(credentialsProvider(options, shared))
      .overrideConfiguration(ClientOverrideConfiguration.builder().retryPolicy(RetryMode.STANDARD).build())
    options.region.foreach(r => b.region(Region.of(r)))
    options.endpoint.foreach(e => b.endpointOverride(URI.create(e)))
    shared.foreach(b.httpClient)
    b.build()
  }

  /** The SDK default client when no http-client.* key is set. The SDK does not close a client it is handed and gluecat
    * never closes catalogs, so there is nothing to release. */
  def httpClient(options: GlueCatalogOptions): Option[SdkHttpClient] = {
    val h = options.httpClient
    h.effectiveType.map { t =>
      def ms(v: Option[Long]) = v.map(Duration.ofMillis)
      def ignored(keys: Seq[(String, Boolean)]): Unit = {
        val names = keys.collect { case (k, true) => k }
        if (names.nonEmpty) log.warn(s"http-client.type is '$t': ignoring ${names.mkString(", ")}")
      }
      try t match {
        case "apache" =>
          ignored(Seq(
            "http-client.urlconnection.socket-timeout-ms" -> h.urlConnectionSocketTimeoutMs.isDefined,
            "http-client.urlconnection.connection-timeout-ms" -> h.urlConnectionConnectionTimeoutMs.isDefined))
          val b = ApacheHttpClient.builder()
          ms(h.apacheSocketTimeoutMs).foreach(b.socketTimeout)
          ms(h.apacheConnectionTimeoutMs).foreach(b.connectionTimeout)
          ms(h.apacheConnectionAcquisitionTimeoutMs).foreach(b.connectionAcquisitionTimeout)
          ms(h.apacheConnectionMaxIdleTimeMs).foreach(b.connectionMaxIdleTime)
          ms(h.apacheConnectionTimeToLiveMs).foreach(b.connectionTimeToLive)
          h.apacheExpectContinueEnabled.foreach(v => b.expectContinueEnabled(v))
          h.apacheMaxConnections.foreach(v => b.maxConnections(v))
          h.apacheTcpKeepAliveEnabled.foreach(v => b.tcpKeepAlive(v))
          h.apacheUseIdleConnectionReaperEnabled.foreach(v => b.useIdleConnectionReaper(v))
          if (Seq(h.proxyEndpoint, h.proxyUseSystemPropertyValues, h.proxyUseEnvironmentVariableValues).exists(_.isDefined)) {
            val p = ApacheProxy.builder()
            h.proxyEndpoint.foreach(e => p.endpoint(URI.create(e)))
            h.proxyUseSystemPropertyValues.foreach(v => p.useSystemPropertyValues(v))
            h.proxyUseEnvironmentVariableValues.foreach(v => p.useEnvironmentVariableValues(v))
            b.proxyConfiguration(p.build())
          }
          b.build()
        case _ =>
          ignored(Seq(
            "http-client.apache.socket-timeout-ms" -> h.apacheSocketTimeoutMs.isDefined,
            "http-client.apache.connection-timeout-ms" -> h.apacheConnectionTimeoutMs.isDefined,
            "http-client.apache.connection-acquisition-timeout-ms" -> h.apacheConnectionAcquisitionTimeoutMs.isDefined,
            "http-client.apache.connection-max-idle-time-ms" -> h.apacheConnectionMaxIdleTimeMs.isDefined,
            "http-client.apache.connection-time-to-live-ms" -> h.apacheConnectionTimeToLiveMs.isDefined,
            "http-client.apache.expect-continue-enabled" -> h.apacheExpectContinueEnabled.isDefined,
            "http-client.apache.max-connections" -> h.apacheMaxConnections.isDefined,
            "http-client.apache.tcp-keep-alive-enabled" -> h.apacheTcpKeepAliveEnabled.isDefined,
            "http-client.apache.use-idle-connection-reaper-enabled" -> h.apacheUseIdleConnectionReaperEnabled.isDefined))
          val b = UrlConnectionHttpClient.builder()
          ms(h.urlConnectionSocketTimeoutMs).foreach(b.socketTimeout)
          ms(h.urlConnectionConnectionTimeoutMs).foreach(b.connectionTimeout)
          if (Seq(h.proxyEndpoint, h.proxyUseSystemPropertyValues, h.proxyUseEnvironmentVariableValues).exists(_.isDefined)) {
            val p = UrlConnectionProxy.builder()
            h.proxyEndpoint.foreach(e => p.endpoint(URI.create(e)))
            h.proxyUseSystemPropertyValues.foreach(v => p.useSystemPropertyValues(v))
            // The SDK spells it differently here than in the apache ProxyConfiguration.
            h.proxyUseEnvironmentVariableValues.foreach(v => p.useEnvironmentVariablesValues(v))
            b.proxyConfiguration(p.build())
          }
          b.build()
      } catch {
        // The SDK HTTP clients are optional dependencies: the missing jar surfaces here as a linkage error.
        case _: NoClassDefFoundError | _: ClassNotFoundException =>
          val artifact = if (t == "apache") "apache-client" else "url-connection-client"
          throw new IllegalArgumentException(
            s"http-client.type '$t' needs software.amazon.awssdk:$artifact on the classpath")
      }
    }
  }

  /** Precedence: custom class, then assume role, then the default chain. */
  def credentialsProvider(options: GlueCatalogOptions, http: Option[SdkHttpClient] = None): AwsCredentialsProvider =
    options.credentialsProvider.map(custom(_, options))
      .orElse(options.assumeRoleArn.map(assumeRole(_, options, http)))
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

  private def assumeRole(arn: String, options: GlueCatalogOptions, http: Option[SdkHttpClient]): AwsCredentialsProvider = {
    val sts = StsClient.builder().credentialsProvider(defaultChain())
    http.foreach(sts.httpClient)
    options.assumeRoleRegion.orElse(options.region).foreach(r => sts.region(Region.of(r)))
    val req = AssumeRoleRequest.builder().roleArn(arn).roleSessionName(options.assumeRoleSessionName)
      .durationSeconds(options.assumeRoleTimeoutSec)
    options.assumeRoleExternalId.foreach(req.externalId)
    // No asyncCredentialUpdateEnabled: Spark never closes catalog plugins, and the SDK's background refresher keeps
    // the provider and its StsClient reachable, so every abandoned session would keep an AssumeRole loop running.
    // The default refreshes on the calling thread within the 5-minute prefetch window.
    StsAssumeRoleCredentialsProvider.builder().stsClient(sts.build()).refreshRequest(req.build()).build()
  }
}
