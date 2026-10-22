package io.github.sdaberdaku.gluecat.glue

final case class GlueColumn(name: String, dataType: String, comment: Option[String] = None)

final case class GlueStorage(location: Option[String], inputFormat: Option[String], outputFormat: Option[String],
    serdeLibrary: Option[String], serdeParameters: Map[String, String], columns: Seq[GlueColumn])

final case class GlueTableEntry(database: String, name: String, tableType: Option[String], description: Option[String],
    owner: Option[String], parameters: Map[String, String], storage: GlueStorage, partitionKeys: Seq[GlueColumn],
    versionId: Option[String])

final case class GlueDatabaseEntry(name: String, description: Option[String], locationUri: Option[String],
    parameters: Map[String, String])

sealed abstract class GlueApiException(message: String, cause: Throwable) extends RuntimeException(message, cause)
final class GlueEntityNotFound(message: String, cause: Throwable = null) extends GlueApiException(message, cause)
final class GlueAlreadyExists(message: String, cause: Throwable = null) extends GlueApiException(message, cause)
final class GlueConcurrentModification(message: String, cause: Throwable = null) extends GlueApiException(message, cause)
final class GlueAccessDenied(message: String, cause: Throwable = null) extends GlueApiException(message, cause)
final class GlueFailure(val operation: String, message: String, cause: Throwable = null)
    extends GlueApiException(message, cause)
