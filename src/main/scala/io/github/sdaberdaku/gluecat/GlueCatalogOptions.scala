package io.github.sdaberdaku.gluecat

import scala.collection.JavaConverters._

import org.slf4j.LoggerFactory

final case class GlueCatalogOptions(
    glueId: Option[String], region: Option[String], endpoint: Option[String], skipArchive: Boolean,
    warehouse: Option[String], defaultNamespace: String,
    credentialsProvider: Option[String], credentialsProviderOptions: Map[String, String],
    assumeRoleArn: Option[String], assumeRoleExternalId: Option[String], assumeRoleSessionName: String,
    assumeRoleRegion: Option[String], assumeRoleTimeoutSec: Int)

object GlueCatalogOptions {
  private val log = LoggerFactory.getLogger(getClass)
  private val ProviderPrefix = "client.credentials-provider."
  private val Known = Set(
    "glue.id", "glue.endpoint", "glue.skip-archive", "warehouse", "default-namespace",
    "client.region", "client.credentials-provider", "client.assume-role.arn",
    "client.assume-role.external-id", "client.assume-role.session-name",
    "client.assume-role.region", "client.assume-role.timeout-sec")

  def parse(catalogName: String, options: java.util.Map[String, String]): GlueCatalogOptions = {
    val all = options.asScala.toMap
    // Blank values count as absent.
    val opts = all.collect { case (k, v) if v != null && v.trim.nonEmpty => k -> v.trim }
    def get(k: String): Option[String] = opts.get(k)

    all.keys.filter(k => (k.startsWith("glue.") || k.startsWith("client.")) &&
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
      assumeRoleTimeoutSec = timeout)
  }
}
