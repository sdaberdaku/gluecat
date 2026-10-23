package io.github.sdaberdaku.gluecat

/** S3A settings derived from the catalog's assume-role options, so data access uses the same role as Glue. */
object S3RoleDefaults {
  private val Provider = "fs.s3a.aws.credentials.provider"
  private val BaseProvider = "fs.s3a.assumed.role.credentials.provider"
  private val AssumedRoleProvider = "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider"

  def derive(options: GlueCatalogOptions, sessionHadoop: String => Option[String]): Map[String, String] =
    options.assumeRoleArn match {
      case Some(arn) if options.credentialsProvider.isEmpty =>
        val sessionProvider = sessionHadoop(Provider)
        val base = sessionProvider.filter(p => p != AssumedRoleProvider && sessionHadoop(BaseProvider).isEmpty)
        Map(
          Provider -> AssumedRoleProvider,
          "fs.s3a.assumed.role.arn" -> arn,
          "fs.s3a.assumed.role.session.name" -> options.assumeRoleSessionName,
          "fs.s3a.assumed.role.session.duration" -> s"${options.assumeRoleTimeoutSec}s") ++
          options.assumeRoleExternalId.map("fs.s3a.assumed.role.external.id" -> _) ++
          options.region.map("fs.s3a.endpoint.region" -> _) ++
          base.map(BaseProvider -> _)
      case _ => Map.empty
    }

  /** Reads only what the user set: keys whose sole source is Hadoop's built-in core-default.xml count as unset. */
  def userSet(conf: org.apache.hadoop.conf.Configuration): String => Option[String] = k => {
    val sources = conf.getPropertySources(k)
    if (sources == null || sources.forall(_ == "core-default.xml")) None else Option(conf.get(k))
  }

  /** Derived defaults, overridden by explicit `hadoop.*` options. */
  def effective(options: GlueCatalogOptions, sessionHadoop: String => Option[String]): Map[String, String] =
    derive(options, sessionHadoop) ++ options.hadoop
}
