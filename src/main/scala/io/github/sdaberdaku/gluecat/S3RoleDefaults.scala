package io.github.sdaberdaku.gluecat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.VersionInfo

/** Two views of the session's Hadoop configuration: what the user set, and the effective value (with core-default.xml). */
final case class SessionHadoop(userSet: String => Option[String], effective: String => Option[String])

object SessionHadoop {
  def apply(conf: Configuration): SessionHadoop = SessionHadoop(
    k => {
      val sources = conf.getPropertySources(k)
      if (sources == null || sources.forall(_ == "core-default.xml")) None else Option(conf.get(k))
    },
    k => Option(conf.get(k)))
}

/** S3A settings derived from the catalog's assume-role options, so data access uses the same role as Glue. */
object S3RoleDefaults {
  private val Provider = "fs.s3a.aws.credentials.provider"
  private val BaseProvider = "fs.s3a.assumed.role.credentials.provider"
  private val AssumedRoleProvider = "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider"

  private def externalIdInPlay(o: GlueCatalogOptions) =
    o.assumeRoleArn.isDefined && o.credentialsProvider.isEmpty && o.assumeRoleExternalId.isDefined

  /** None when the version string cannot be parsed (e.g. "Unknown" from repackaged Hadoop jars). */
  private def below34(version: String): Option[Boolean] = scala.util.Try {
    val Array(major, minor) = version.split("\\.").take(2).map(_.takeWhile(_.isDigit).toInt)
    major < 3 || (major == 3 && minor < 4)
  }.toOption

  /** hadoop-aws before 3.4 cannot send an external ID to STS; an unparseable version counts as supporting it. */
  def externalIdUnsupported(options: GlueCatalogOptions, hadoopVersion: String = VersionInfo.getVersion): Boolean =
    externalIdInPlay(options) && below34(hadoopVersion).getOrElse(false)

  /** Warnings for the catalog `name`, at most one. */
  def warnings(name: String, options: GlueCatalogOptions, hadoopVersion: String = VersionInfo.getVersion): Seq[String] =
    if (!externalIdInPlay(options)) Nil
    else below34(hadoopVersion) match {
      case None => Seq(s"gluecat catalog '$name': Hadoop version '$hadoopVersion' could not be parsed; assuming " +
        "S3A can send client.assume-role.external-id to STS.")
      case Some(true) => Seq(s"gluecat catalog '$name': Hadoop $hadoopVersion cannot send " +
        "client.assume-role.external-id to STS for S3A; no S3A defaults derived. Point " +
        "hadoop.fs.s3a.assumed.role.arn at a data role without an external-id condition, or use Spark 4.")
      case Some(false) => Nil
    }

  def derive(options: GlueCatalogOptions, session: SessionHadoop,
      hadoopVersion: String = VersionInfo.getVersion): Map[String, String] =
    options.assumeRoleArn match {
      case Some(arn) if options.credentialsProvider.isEmpty && !externalIdUnsupported(options, hadoopVersion) =>
        val base = if (session.userSet(BaseProvider).nonEmpty) None else {
          val kept = session.effective(Provider).toSeq.flatMap(_.split(",")).map(_.trim)
            .filter(p => p.nonEmpty && p != AssumedRoleProvider && !p.endsWith("." + AssumedRoleProvider.split("\\.").last))
          if (kept.isEmpty) None else Some(kept.mkString(","))
        }
        Map(
          Provider -> AssumedRoleProvider,
          "fs.s3a.assumed.role.arn" -> arn,
          "fs.s3a.assumed.role.session.name" -> options.assumeRoleSessionName,
          "fs.s3a.assumed.role.session.duration" -> s"${options.assumeRoleTimeoutSec}s") ++
          options.assumeRoleExternalId.map("fs.s3a.assumed.role.external.id" -> _) ++
          base.map(BaseProvider -> _)
      case _ => Map.empty
    }

  /** Derived defaults, overridden by explicit `hadoop.*` options. */
  def effective(options: GlueCatalogOptions, session: SessionHadoop,
      hadoopVersion: String = VersionInfo.getVersion): Map[String, String] =
    derive(options, session, hadoopVersion) ++ options.hadoop
}
