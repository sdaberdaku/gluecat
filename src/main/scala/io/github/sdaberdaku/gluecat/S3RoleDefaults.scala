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

  /** hadoop-aws before 3.4 cannot send an external ID to STS. */
  def externalIdUnsupported(options: GlueCatalogOptions, hadoopVersion: String = VersionInfo.getVersion): Boolean = {
    val Array(major, minor) = hadoopVersion.split("\\.").take(2).map(_.takeWhile(_.isDigit).toInt)
    options.assumeRoleArn.isDefined && options.credentialsProvider.isEmpty &&
      options.assumeRoleExternalId.isDefined && (major < 3 || (major == 3 && minor < 4))
  }

  def derive(options: GlueCatalogOptions, session: SessionHadoop,
      hadoopVersion: String = VersionInfo.getVersion): Map[String, String] =
    options.assumeRoleArn match {
      case Some(arn) if options.credentialsProvider.isEmpty && !externalIdUnsupported(options, hadoopVersion) =>
        val base = if (session.userSet(BaseProvider).nonEmpty) None else {
          val kept = session.effective(Provider).toSeq.flatMap(_.split(",")).map(_.trim)
            .filter(p => p.nonEmpty && !p.endsWith("AssumedRoleCredentialProvider"))
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
