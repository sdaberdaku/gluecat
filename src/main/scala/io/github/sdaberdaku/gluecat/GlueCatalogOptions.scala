package io.github.sdaberdaku.gluecat

import scala.collection.JavaConverters._

import org.slf4j.LoggerFactory

final case class GlueCatalogOptions(
    glueId: Option[String], region: Option[String], endpoint: Option[String], skipArchive: Boolean,
    warehouse: Option[String], defaultNamespace: String,
    credentialsProvider: Option[String], credentialsProviderOptions: Map[String, String],
    assumeRoleArn: Option[String], assumeRoleExternalId: Option[String], assumeRoleSessionName: String,
    assumeRoleRegion: Option[String], assumeRoleTimeoutSec: Int,
    hadoop: Map[String, String], httpClient: HttpClientOptions)

final case class HttpClientOptions(
    clientType: Option[String], proxyEndpoint: Option[String],
    proxyUseSystemPropertyValues: Option[Boolean], proxyUseEnvironmentVariableValues: Option[Boolean],
    urlConnectionSocketTimeoutMs: Option[Long], urlConnectionConnectionTimeoutMs: Option[Long],
    apacheSocketTimeoutMs: Option[Long], apacheConnectionTimeoutMs: Option[Long],
    apacheConnectionAcquisitionTimeoutMs: Option[Long], apacheConnectionMaxIdleTimeMs: Option[Long],
    apacheConnectionTimeToLiveMs: Option[Long], apacheExpectContinueEnabled: Option[Boolean],
    apacheMaxConnections: Option[Int], apacheTcpKeepAliveEnabled: Option[Boolean],
    apacheUseIdleConnectionReaperEnabled: Option[Boolean]) {
  // Proxy settings apply to both client types; on their own they imply apache.
  private[gluecat] def hasProxyKeys: Boolean =
    Seq(proxyEndpoint, proxyUseSystemPropertyValues, proxyUseEnvironmentVariableValues).exists(_.isDefined)
  private[gluecat] def hasApacheKeys: Boolean = Seq(
    apacheSocketTimeoutMs, apacheConnectionTimeoutMs, apacheConnectionAcquisitionTimeoutMs,
    apacheConnectionMaxIdleTimeMs, apacheConnectionTimeToLiveMs, apacheExpectContinueEnabled, apacheMaxConnections,
    apacheTcpKeepAliveEnabled, apacheUseIdleConnectionReaperEnabled).exists(_.isDefined)
  private[gluecat] def hasUrlConnectionKeys: Boolean =
    urlConnectionSocketTimeoutMs.isDefined || urlConnectionConnectionTimeoutMs.isDefined

  def effectiveType: Option[String] = clientType.orElse {
    if (hasApacheKeys) Some("apache")
    else if (hasUrlConnectionKeys) Some("urlconnection")
    else if (hasProxyKeys) Some("apache")
    else None
  }
}

object GlueCatalogOptions {
  private val log = LoggerFactory.getLogger(getClass)
  private val ProviderPrefix = "client.credentials-provider."
  private val HadoopPrefix = "hadoop."
  private val HttpPrefix = "http-client."
  private val Known = Set(
    "glue.id", "glue.endpoint", "glue.skip-archive", "warehouse", "default-namespace",
    "client.region", "client.credentials-provider", "client.assume-role.arn",
    "client.assume-role.external-id", "client.assume-role.session-name",
    "client.assume-role.region", "client.assume-role.timeout-sec",
    "http-client.type", "http-client.proxy-endpoint", "http-client.proxy-use-system-property-values",
    "http-client.proxy-use-environment-variable-values",
    "http-client.urlconnection.socket-timeout-ms", "http-client.urlconnection.connection-timeout-ms",
    "http-client.apache.socket-timeout-ms", "http-client.apache.connection-timeout-ms",
    "http-client.apache.connection-acquisition-timeout-ms", "http-client.apache.connection-max-idle-time-ms",
    "http-client.apache.connection-time-to-live-ms", "http-client.apache.expect-continue-enabled",
    "http-client.apache.max-connections", "http-client.apache.tcp-keep-alive-enabled",
    "http-client.apache.use-idle-connection-reaper-enabled")

  def parse(catalogName: String, options: java.util.Map[String, String]): GlueCatalogOptions = {
    val all = options.asScala.toMap
    // Blank values count as absent.
    val opts = all.collect { case (k, v) if v != null && v.trim.nonEmpty => k -> v.trim }
    def get(k: String): Option[String] = opts.get(k)

    all.keys.filter(k => (k.startsWith("glue.") || k.startsWith("client.") || k.startsWith(HttpPrefix)) &&
      !Known.contains(k) && !k.startsWith(ProviderPrefix)).toSeq.sorted.foreach { k =>
      log.warn(s"Catalog '$catalogName': ignoring unknown option '$k'")
    }

    val namespace = get("default-namespace").getOrElse("default")
    require(!namespace.contains("."), s"default-namespace must be a single level, got '$namespace'")

    val timeout = get("client.assume-role.timeout-sec").map { v =>
      val n = scala.util.Try(v.toInt).getOrElse(-1)
      require(n > 0, s"client.assume-role.timeout-sec must be a positive integer, got '$v'")
      n
    }.getOrElse(3600)

    def num[T](k: String, conv: String => T): Option[T] = get(k).map { v =>
      try conv(v) catch { case _: NumberFormatException =>
        throw new IllegalArgumentException(s"$k must be an integer, got '$v'") }
    }
    def long(k: String): Option[Long] = num(k, _.toLong)
    def bool(k: String): Option[Boolean] = get(k).map(_.toLowerCase.toBoolean)
    val http = HttpClientOptions(
      clientType = get("http-client.type"),
      proxyEndpoint = get("http-client.proxy-endpoint"),
      proxyUseSystemPropertyValues = bool("http-client.proxy-use-system-property-values"),
      proxyUseEnvironmentVariableValues = bool("http-client.proxy-use-environment-variable-values"),
      urlConnectionSocketTimeoutMs = long("http-client.urlconnection.socket-timeout-ms"),
      urlConnectionConnectionTimeoutMs = long("http-client.urlconnection.connection-timeout-ms"),
      apacheSocketTimeoutMs = long("http-client.apache.socket-timeout-ms"),
      apacheConnectionTimeoutMs = long("http-client.apache.connection-timeout-ms"),
      apacheConnectionAcquisitionTimeoutMs = long("http-client.apache.connection-acquisition-timeout-ms"),
      apacheConnectionMaxIdleTimeMs = long("http-client.apache.connection-max-idle-time-ms"),
      apacheConnectionTimeToLiveMs = long("http-client.apache.connection-time-to-live-ms"),
      apacheExpectContinueEnabled = bool("http-client.apache.expect-continue-enabled"),
      apacheMaxConnections = num("http-client.apache.max-connections", _.toInt),
      apacheTcpKeepAliveEnabled = bool("http-client.apache.tcp-keep-alive-enabled"),
      apacheUseIdleConnectionReaperEnabled = bool("http-client.apache.use-idle-connection-reaper-enabled"))
    http.clientType.foreach { t =>
      require(t == "apache" || t == "urlconnection", s"http-client.type must be 'apache' or 'urlconnection', got '$t'")
    }
    require(http.clientType.isDefined || !(http.hasApacheKeys && http.hasUrlConnectionKeys),
      "http-client.*: settings for both apache and urlconnection are present; set http-client.type")

    GlueCatalogOptions(
      glueId = get("glue.id"),
      region = get("client.region"),
      endpoint = get("glue.endpoint"),
      skipArchive = get("glue.skip-archive").map(_.toLowerCase.toBoolean).getOrElse(true),
      warehouse = get("warehouse"),
      defaultNamespace = namespace,
      credentialsProvider = get("client.credentials-provider"),
      credentialsProviderOptions = opts.collect {
        case (k, v) if k.startsWith(ProviderPrefix) => k.substring(ProviderPrefix.length) -> v
      },
      assumeRoleArn = get("client.assume-role.arn"),
      assumeRoleExternalId = get("client.assume-role.external-id"),
      assumeRoleSessionName = get("client.assume-role.session-name").getOrElse("gluecat"),
      assumeRoleRegion = get("client.assume-role.region"),
      assumeRoleTimeoutSec = timeout,
      hadoop = opts.collect { case (k, v) if k.startsWith(HadoopPrefix) => k.substring(HadoopPrefix.length) -> v },
      httpClient = http)
  }
}
